#include "clientid.h"
#include "counter.h"
#include "element.h"
#include "elementid.h"
#include "map.h"
#include "register.h"
#include "scalar.h"
#include "stamp.h"
#include "string.h"
#include "test_util.h"
#include <stdio.h>

// NOTE: targets the refcounted "Share" lifecycle contract (no arena). Will not
// link until map.c (and element.c) are converted. Expected Map surface:
//   Map *map_create(ElementId id);                 // refcount = 1
//   void map_acquire(Map *);
//   void map_release(Map *);   // drops slot composite refs (recursive), frees
//   void map_displace(Map *);
//   bool map_is_displaced(const Map *);
//   Map *map_clone(const Map *);                   // refcount = 1, deep copy
//
// Share semantics, exercised throughout:
//   - map_set of a composite: if the write is ACCEPTED (LWW wins), the Map
//     element_acquires its own ref on the composite. If REJECTED, no-op. Either
//     way the caller still owns the handle it passed and must release it.
//   - map_get and the helper INSTALL path return BORROWS (slot keeps owning the
//     ref). To keep a borrowed handle valid past the next eviction, acquire it.
//   - Eviction (winning set/delete over a live composite, or merge LWW-replace)
//     displaces + releases the slot's ref on the loser.
//   - Helper DETACHED path (stamp loses LWW) returns an OWNED rc=1 handle that
//     is born displaced; the caller must release it.

static ClientId cid(uint8_t first_byte) {
    uint8_t b[16] = {0};
    b[0] = first_byte;
    return clientid_from_bytes(b);
}

static ElementId eid(uint64_t hi, uint64_t lo) {
    uint8_t b[16];
    for (int i = 0; i < 8; i++) {
        b[i] = (uint8_t)((hi >> ((7 - i) * 8)) & 0xff);
        b[8 + i] = (uint8_t)((lo >> ((7 - i) * 8)) & 0xff);
    }
    return elementid_from_bytes(b);
}

// Default id for tests where the parent Map's identity does not matter.
static ElementId default_id(void) { return eid(0xFF, 0); }

static Stamp stmp(uint64_t lamport, uint8_t client_first_byte) {
    return (Stamp){.lamport = lamport, .client_id = cid(client_first_byte)};
}

// String-key shorthand: expands to (bytes, length) without the NUL terminator.
#define SK(s) ((const void *)(s)), strlen(s)

// Element wrappers for readability at the call site.
#define EI(n) element_scalar(scalar_int(n))
#define ES(p, n) element_scalar(scalar_string((const uint8_t *)(p), (n)))

static Map *fresh(void) { return map_create(default_id()); }

TEST(map_create_stores_id) {
    ElementId id = eid(7, 42);
    Map *m = map_create(id);
    ASSERT(elementid_eq(map_id(m), id) == true);
    map_release(m);
}

#define ASSERT_SCALAR_EQ(out, expected)                                        \
    do {                                                                       \
        ASSERT_EQ(element_kind(out), ELEMENT_SCALAR);                          \
        ASSERT(scalar_eq((out).as.scalar, (expected)));                        \
    } while (0)

// --- local set / get (scalar slots) ---

TEST(empty_get_returns_false) {
    Map *m = fresh();
    Element out;
    ASSERT(map_get(m, SK("missing"), &out) == false);
    map_release(m);
}

TEST(set_then_get) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(42), stmp(1, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(42));
    map_release(m);
}

TEST(set_overwrites_with_newer_stamp) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(10), stmp(1, 1));
    map_set(m, SK("k"), EI(20), stmp(2, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
    map_release(m);
}

TEST(set_lower_stamp_ignored) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(20), stmp(5, 1));
    map_set(m, SK("k"), EI(10), stmp(3, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
    map_release(m);
}

TEST(set_equal_lamport_higher_client_wins) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(10), stmp(5, 1));
    map_set(m, SK("k"), EI(20), stmp(5, 2));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
    map_release(m);
}

TEST(set_equal_lamport_lower_client_ignored) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(20), stmp(5, 2));
    map_set(m, SK("k"), EI(10), stmp(5, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
    map_release(m);
}

TEST(set_same_stamp_idempotent) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(42), stmp(5, 1));
    map_set(m, SK("k"), EI(42), stmp(5, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(42));
    map_release(m);
}

TEST(set_can_change_value_kind) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(42), stmp(1, 1));
    map_set(m, SK("k"), ES("hi", 2), stmp(2, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_string((const uint8_t *)"hi", 2));
    map_release(m);
}

TEST(distinct_keys_are_independent) {
    Map *m = fresh();
    map_set(m, SK("a"), EI(1), stmp(1, 1));
    map_set(m, SK("b"), EI(2), stmp(1, 1));
    Element a, b;
    ASSERT(map_get(m, SK("a"), &a) == true);
    ASSERT(map_get(m, SK("b"), &b) == true);
    ASSERT_SCALAR_EQ(a, scalar_int(1));
    ASSERT_SCALAR_EQ(b, scalar_int(2));
    map_release(m);
}

TEST(keys_with_embedded_nul_are_distinct) {
    Map *m = fresh();
    uint8_t k1[3] = {0x01, 0x00, 0x02};
    uint8_t k2[3] = {0x01, 0x00, 0x03};
    map_set(m, k1, sizeof k1, EI(1), stmp(1, 1));
    map_set(m, k2, sizeof k2, EI(2), stmp(1, 1));
    Element v1, v2;
    ASSERT(map_get(m, k1, sizeof k1, &v1) == true);
    ASSERT(map_get(m, k2, sizeof k2, &v2) == true);
    ASSERT_SCALAR_EQ(v1, scalar_int(1));
    ASSERT_SCALAR_EQ(v2, scalar_int(2));
    map_release(m);
}

// --- delete / tombstones ---

TEST(delete_makes_get_return_false) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(42), stmp(1, 1));
    map_delete(m, SK("k"), stmp(2, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == false);
    map_release(m);
}

TEST(delete_with_lower_stamp_ignored) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(42), stmp(5, 1));
    map_delete(m, SK("k"), stmp(3, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(42));
    map_release(m);
}

TEST(set_after_delete_with_higher_stamp_resurrects) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(10), stmp(1, 1));
    map_delete(m, SK("k"), stmp(2, 1));
    map_set(m, SK("k"), EI(20), stmp(3, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
    map_release(m);
}

TEST(set_after_delete_with_lower_stamp_ignored) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(10), stmp(1, 1));
    map_delete(m, SK("k"), stmp(5, 1));
    map_set(m, SK("k"), EI(20), stmp(3, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == false);
    map_release(m);
}

TEST(set_vs_delete_higher_stamp_wins_delete) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(10), stmp(1, 1));
    map_delete(m, SK("k"), stmp(5, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == false);
    map_release(m);
}

TEST(delete_idempotent_same_stamp) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(10), stmp(1, 1));
    map_delete(m, SK("k"), stmp(5, 1));
    map_delete(m, SK("k"), stmp(5, 1));
    Element out;
    ASSERT(map_get(m, SK("k"), &out) == false);
    map_release(m);
}

TEST(delete_absent_key_still_installs_tombstone) {
    Map *m = fresh();
    map_delete(m, SK("ghost"), stmp(10, 1));
    map_set(m, SK("ghost"), EI(1), stmp(5, 1));
    Element out;
    ASSERT(map_get(m, SK("ghost"), &out) == false);
    map_release(m);
}

// --- map_size ---

TEST(size_zero_initially) {
    Map *m = fresh();
    ASSERT_EQ(map_size(m), 0);
    map_release(m);
}

TEST(size_counts_live_entries) {
    Map *m = fresh();
    map_set(m, SK("a"), EI(1), stmp(1, 1));
    map_set(m, SK("b"), EI(2), stmp(1, 1));
    map_set(m, SK("c"), EI(3), stmp(1, 1));
    ASSERT_EQ(map_size(m), 3);
    map_release(m);
}

TEST(size_excludes_tombstones) {
    Map *m = fresh();
    map_set(m, SK("a"), EI(1), stmp(1, 1));
    map_set(m, SK("b"), EI(2), stmp(1, 1));
    map_delete(m, SK("b"), stmp(2, 1));
    ASSERT_EQ(map_size(m), 1);
    map_release(m);
}

TEST(size_recovers_on_resurrect) {
    Map *m = fresh();
    map_set(m, SK("k"), EI(1), stmp(1, 1));
    map_delete(m, SK("k"), stmp(2, 1));
    ASSERT_EQ(map_size(m), 0);
    map_set(m, SK("k"), EI(2), stmp(3, 1));
    ASSERT_EQ(map_size(m), 1);
    map_release(m);
}

// --- composite slot reads ---
//
// Pattern: create the composite (rc=1), map_set installs it (Map acquires its
// own ref, rc=2), then the caller releases its handle (rc=1, Map owns). The
// pointer stays valid because the Map still holds a ref; map_release frees it.

TEST(set_counter_then_get_returns_element_counter) {
    Map *m = fresh();
    Counter *c = counter_create(default_id());
    counter_inc(c, cid(1), 5);
    map_set(m, SK("votes"), element_counter(c), stmp(1, 1));
    counter_release(c); // Map now owns the sole ref

    Element out;
    ASSERT(map_get(m, SK("votes"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT(out.as.counter == c);
    ASSERT_EQ(counter_read(out.as.counter), 5);
    map_release(m);
}

TEST(set_register_then_get_returns_element_register) {
    Map *m = fresh();
    Register *r = register_create(default_id(), scalar_int(7), stmp(1, 1));
    map_set(m, SK("title"), element_register(r), stmp(1, 1));
    register_release(r);

    Element out;
    ASSERT(map_get(m, SK("title"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_REGISTER);
    ASSERT(scalar_eq(register_read(out.as.reg), scalar_int(7)));
    map_release(m);
}

TEST(set_nested_map_then_get_returns_element_map) {
    Map *outer = fresh();
    Map *inner = map_create(default_id());
    map_set(inner, SK("a"), EI(1), stmp(1, 1));
    map_set(outer, SK("child"), element_map(inner), stmp(1, 1));
    map_release(inner); // outer owns the sole ref

    Element out;
    ASSERT(map_get(outer, SK("child"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_MAP);
    Element inner_out;
    ASSERT(map_get(out.as.map, SK("a"), &inner_out) == true);
    ASSERT_SCALAR_EQ(inner_out, scalar_int(1));
    map_release(outer); // recursively releases inner
}

// --- merge (two replicas, scalar slots) ---

TEST(merge_disjoint_keys_unions) {
    Map *a = fresh();
    Map *b = fresh();
    map_set(a, SK("x"), EI(1), stmp(1, 1));
    map_set(b, SK("y"), EI(2), stmp(1, 2));

    map_merge(a, b);
    Element x, y;
    ASSERT(map_get(a, SK("x"), &x) == true);
    ASSERT(map_get(a, SK("y"), &y) == true);
    ASSERT_SCALAR_EQ(x, scalar_int(1));
    ASSERT_SCALAR_EQ(y, scalar_int(2));
    ASSERT_EQ(map_size(a), 2);
    map_release(a);
    map_release(b);
}

TEST(merge_same_key_newer_wins) {
    Map *a = fresh();
    Map *b = fresh();
    map_set(a, SK("k"), EI(10), stmp(1, 1));
    map_set(b, SK("k"), EI(20), stmp(2, 2));

    map_merge(a, b);
    Element out;
    ASSERT(map_get(a, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
    map_release(a);
    map_release(b);
}

TEST(merge_src_older_loses) {
    Map *a = fresh();
    Map *b = fresh();
    map_set(a, SK("k"), EI(20), stmp(5, 1));
    map_set(b, SK("k"), EI(10), stmp(2, 2));

    map_merge(a, b);
    Element out;
    ASSERT(map_get(a, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(20));
    map_release(a);
    map_release(b);
}

TEST(merge_delete_beats_older_set) {
    Map *a = fresh();
    Map *b = fresh();
    map_set(a, SK("k"), EI(10), stmp(1, 1));
    map_delete(b, SK("k"), stmp(5, 1));

    map_merge(a, b);
    Element out;
    ASSERT(map_get(a, SK("k"), &out) == false);
    map_release(a);
    map_release(b);
}

TEST(merge_set_beats_older_delete) {
    Map *a = fresh();
    Map *b = fresh();
    map_delete(a, SK("k"), stmp(1, 1));
    map_set(b, SK("k"), EI(42), stmp(5, 1));

    map_merge(a, b);
    Element out;
    ASSERT(map_get(a, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(42));
    map_release(a);
    map_release(b);
}

TEST(merge_commutative) {
    Map *a1 = fresh();
    Map *b1 = fresh();
    map_set(a1, SK("k"), EI(10), stmp(5, 1));
    map_set(b1, SK("k"), EI(20), stmp(5, 2));
    map_merge(a1, b1);

    Map *a2 = fresh();
    Map *b2 = fresh();
    map_set(a2, SK("k"), EI(10), stmp(5, 1));
    map_set(b2, SK("k"), EI(20), stmp(5, 2));
    map_merge(b2, a2);

    Element v1, v2;
    ASSERT(map_get(a1, SK("k"), &v1) == true);
    ASSERT(map_get(b2, SK("k"), &v2) == true);
    ASSERT_EQ(element_kind(v1), ELEMENT_SCALAR);
    ASSERT_EQ(element_kind(v2), ELEMENT_SCALAR);
    ASSERT(scalar_eq(v1.as.scalar, v2.as.scalar));
    ASSERT(scalar_eq(v1.as.scalar, scalar_int(20)));
    map_release(a1);
    map_release(b1);
    map_release(a2);
    map_release(b2);
}

TEST(merge_idempotent) {
    Map *a = fresh();
    Map *b = fresh();
    map_set(a, SK("k"), EI(10), stmp(1, 1));
    map_set(b, SK("k"), EI(20), stmp(2, 1));

    map_merge(a, b);
    Element once;
    ASSERT(map_get(a, SK("k"), &once) == true);
    map_merge(a, b);
    Element twice;
    ASSERT(map_get(a, SK("k"), &twice) == true);
    ASSERT_EQ(element_kind(once), ELEMENT_SCALAR);
    ASSERT_EQ(element_kind(twice), ELEMENT_SCALAR);
    ASSERT(scalar_eq(once.as.scalar, twice.as.scalar));
    ASSERT(scalar_eq(twice.as.scalar, scalar_int(20)));
    map_release(a);
    map_release(b);
}

TEST(merge_associative) {
    Map *a = fresh();
    Map *b = fresh();
    Map *c = fresh();
    map_set(a, SK("k"), EI(10), stmp(1, 1));
    map_set(b, SK("k"), EI(20), stmp(2, 1));
    map_set(c, SK("k"), EI(30), stmp(3, 1));
    map_merge(a, b);
    map_merge(a, c);

    Map *a2 = fresh();
    Map *b2 = fresh();
    Map *c2 = fresh();
    map_set(a2, SK("k"), EI(10), stmp(1, 1));
    map_set(b2, SK("k"), EI(20), stmp(2, 1));
    map_set(c2, SK("k"), EI(30), stmp(3, 1));
    map_merge(b2, c2);
    map_merge(a2, b2);

    Element v1, v2;
    ASSERT(map_get(a, SK("k"), &v1) == true);
    ASSERT(map_get(a2, SK("k"), &v2) == true);
    ASSERT_EQ(element_kind(v1), ELEMENT_SCALAR);
    ASSERT_EQ(element_kind(v2), ELEMENT_SCALAR);
    ASSERT(scalar_eq(v1.as.scalar, v2.as.scalar));
    ASSERT(scalar_eq(v1.as.scalar, scalar_int(30)));
    map_release(a);
    map_release(b);
    map_release(c);
    map_release(a2);
    map_release(b2);
    map_release(c2);
}

TEST(merge_does_not_mutate_src) {
    Map *a = fresh();
    Map *b = fresh();
    map_set(a, SK("k"), EI(99), stmp(10, 1));
    map_set(b, SK("k"), EI(7), stmp(1, 1));

    map_merge(a, b);
    Element out;
    ASSERT(map_get(b, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(7));
    map_release(a);
    map_release(b);
}

// Dst must own its own copy of a winning string value: scribbling the source
// buffer after merge must not change what dst reads.
TEST(merge_copies_string_into_dst) {
    Map *a = fresh();
    Map *b = fresh();

    uint8_t src_bytes[8];
    memcpy(src_bytes, "hello", 5);

    map_set(a, SK("k"), EI(0), stmp(1, 1));
    map_set(b, SK("k"), ES(src_bytes, 5), stmp(5, 1));

    map_merge(a, b);

    src_bytes[0] = 'X';
    src_bytes[1] = 'X';

    Element out;
    ASSERT(map_get(a, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_string((const uint8_t *)"hello", 5));
    map_release(a);
    map_release(b);
}

TEST(merge_preserves_tombstone_against_older_set) {
    Map *a = fresh();
    Map *b = fresh();
    map_delete(a, SK("k"), stmp(5, 1));
    map_set(b, SK("k"), EI(10), stmp(2, 1));

    map_merge(a, b);
    Element out;
    ASSERT(map_get(a, SK("k"), &out) == false);
    map_release(a);
    map_release(b);
}

// --- recursive merge: same kind at same key recurses regardless of stamp ---
//
// Position is identity. Two replicas with a composite of the same kind at
// the same key are by definition the same logical object — recurse into
// element_merge. Slot stamp advances to max(dst, src).

TEST(merge_same_kind_counter_recurses) {
    Map *dst = fresh();
    Counter *dc = counter_create(default_id());
    counter_inc(dc, cid(1), 5);
    map_set(dst, SK("votes"), element_counter(dc), stmp(1, 1));
    counter_release(dc);

    Map *src = fresh();
    Counter *sc = counter_create(default_id());
    counter_inc(sc, cid(2), 3);
    map_set(src, SK("votes"), element_counter(sc), stmp(10, 1));
    counter_release(sc);

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("votes"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT(out.as.counter == dc); // dst kept its own pointer
    ASSERT_EQ(counter_read(out.as.counter), 8);
    map_release(dst);
    map_release(src);
}

TEST(merge_same_kind_register_recurses) {
    Map *dst = fresh();
    Register *dr = register_create(default_id(), scalar_int(10), stmp(1, 1));
    map_set(dst, SK("title"), element_register(dr), stmp(1, 1));
    register_release(dr);

    Map *src = fresh();
    Register *sr = register_create(default_id(), scalar_int(20), stmp(5, 1));
    map_set(src, SK("title"), element_register(sr), stmp(1, 1));
    register_release(sr);

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("title"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_REGISTER);
    ASSERT(out.as.reg == dr);
    ASSERT(scalar_eq(register_read(dr), scalar_int(20)));
    map_release(dst);
    map_release(src);
}

TEST(merge_same_kind_nested_map_recurses) {
    Map *dst = fresh();
    Map *di = map_create(default_id());
    map_set(di, SK("a"), EI(1), stmp(1, 1));
    map_set(dst, SK("child"), element_map(di), stmp(1, 1));
    map_release(di);

    Map *src = fresh();
    Map *si = map_create(default_id());
    map_set(si, SK("b"), EI(2), stmp(1, 2));
    map_set(src, SK("child"), element_map(si), stmp(1, 1));
    map_release(si);

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("child"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_MAP);
    ASSERT(out.as.map == di);
    Element a_out, b_out;
    ASSERT(map_get(di, SK("a"), &a_out) == true);
    ASSERT(map_get(di, SK("b"), &b_out) == true);
    ASSERT_SCALAR_EQ(a_out, scalar_int(1));
    ASSERT_SCALAR_EQ(b_out, scalar_int(2));
    map_release(dst);
    map_release(src);
}

TEST(merge_same_kind_counter_does_not_mutate_src) {
    Map *dst = fresh();
    Counter *dc = counter_create(default_id());
    counter_inc(dc, cid(1), 5);
    map_set(dst, SK("votes"), element_counter(dc), stmp(1, 1));
    counter_release(dc);

    Map *src = fresh();
    Counter *sc = counter_create(default_id());
    counter_inc(sc, cid(2), 3);
    map_set(src, SK("votes"), element_counter(sc), stmp(1, 1));

    map_merge(dst, src);

    ASSERT_EQ(counter_read(sc), 3);
    counter_release(sc);
    map_release(dst);
    map_release(src);
}

// Recursive merge must advance the slot stamp to max(dst, src). Otherwise
// future slot-level ops on this key can diverge between replicas. Probe:
// a subsequent set with a stamp above dst's old slot stamp but below src's
// must be rejected.
TEST(merge_same_kind_counter_advances_slot_stamp) {
    Map *dst = fresh();
    Counter *dc = counter_create(default_id());
    counter_inc(dc, cid(1), 5);
    map_set(dst, SK("votes"), element_counter(dc), stmp(1, 1));
    counter_release(dc);

    Map *src = fresh();
    Counter *sc = counter_create(default_id());
    counter_inc(sc, cid(2), 3);
    map_set(src, SK("votes"), element_counter(sc), stmp(10, 1));
    counter_release(sc);

    map_merge(dst, src);

    // A set at stamp(5,1) is below src's slot stamp(10,1) but above dst's
    // old slot stamp(1,1). If dst's stamp wasn't advanced to 10, this set
    // would replace the Counter — and replicas diverge.
    map_set(dst, SK("votes"), EI(99), stmp(5, 1));

    Element out;
    ASSERT(map_get(dst, SK("votes"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT_EQ(counter_read(out.as.counter), 8);
    map_release(dst);
    map_release(src);
}

// --- type-flip via LWW: loser composite is displaced + released ---
//
// A newer-stamped write of a different kind wins the slot. The old composite
// is displaced (its handle marked) and the Map drops its ref. If no other
// holder exists, the loser is freed; tests that want to observe it first
// acquire a ref.

TEST(set_composite_displaces_scalar_at_lww) {
    Map *m = fresh();
    map_set(m, SK("score"), EI(42), stmp(1, 1)); // scalar first
    Counter *c = counter_create(default_id());
    map_set(m, SK("score"), element_counter(c), stmp(5, 1)); // newer composite
    counter_release(c);

    Element out;
    ASSERT(map_get(m, SK("score"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT(out.as.counter == c);
    map_release(m);
}

TEST(set_scalar_displaces_composite_at_lww) {
    Map *m = fresh();
    Counter *c = counter_create(default_id());
    map_set(m, SK("score"), element_counter(c), stmp(1, 1));
    counter_release(c); // Map owns the sole ref; the scalar set below frees it
    map_set(m, SK("score"), EI(42), stmp(5, 1));

    Element out;
    ASSERT(map_get(m, SK("score"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(42));
    map_release(m);
}

TEST(set_different_kind_composite_displaces_at_lww) {
    Map *m = fresh();
    Counter *c = counter_create(default_id());
    map_set(m, SK("score"), element_counter(c), stmp(1, 1));
    counter_release(c);
    Register *r = register_create(default_id(), scalar_int(42), stmp(5, 1));
    map_set(m, SK("score"), element_register(r), stmp(5, 1));
    register_release(r);

    Element out;
    ASSERT(map_get(m, SK("score"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_REGISTER);
    ASSERT(out.as.reg == r);
    map_release(m);
}

// A holder that acquired its own ref before eviction keeps the displaced
// composite alive and observes the displaced flag.
TEST(evicted_composite_is_displaced_and_outlives_via_held_ref) {
    Map *m = fresh();
    Counter *c = counter_create(default_id());
    counter_inc(c, cid(1), 5);
    map_set(m, SK("score"), element_counter(c), stmp(1, 1));
    // Caller keeps its ref (does NOT release) — it wants to observe the
    // eviction. After set the refcount is 2 (caller + Map).

    map_set(m, SK("score"), EI(42), stmp(5, 1)); // evicts the Counter

    // Map dropped its ref; the caller's ref keeps c alive, now displaced.
    ASSERT(counter_is_displaced(c) == true);
    ASSERT_EQ(counter_read(c), 5); // still readable, just orphaned

    counter_release(c); // caller's ref → freed
    map_release(m);
}

// Deleting a live composite slot displaces + releases it, same as an
// overwriting set.
TEST(delete_composite_displaces_it) {
    Map *m = fresh();
    Counter *c = counter_create(default_id());
    map_set(m, SK("score"), element_counter(c), stmp(1, 1));
    // keep caller ref to observe

    map_delete(m, SK("score"), stmp(5, 1));

    Element out;
    ASSERT(map_get(m, SK("score"), &out) == false); // tombstoned
    ASSERT(counter_is_displaced(c) == true);
    counter_release(c);
    map_release(m);
}

// --- cross-replica composite LWW: clone winner, displace+release loser ---

TEST(merge_composite_src_wins_into_empty_slot_clones) {
    Map *dst = fresh();
    Map *src = fresh();
    Counter *sc = counter_create(default_id());
    counter_inc(sc, cid(1), 5);
    map_set(src, SK("votes"), element_counter(sc), stmp(5, 1));
    counter_release(sc);

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("votes"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT(out.as.counter != sc); // dst owns a clone

    map_release(src); // src dies; dst clone must survive
    Element out2;
    ASSERT(map_get(dst, SK("votes"), &out2) == true);
    ASSERT_EQ(counter_read(out2.as.counter), 5);
    map_release(dst);
}

// When src LOSES the LWW comparison, map_merge must NOT clone src's value into
// dst — that clone would be unreachable garbage (a leak in the refcount model).
//
// NOTE: the original arena-based test asserted this via arena_used byte cost.
// There is no refcount equivalent without a host alloc-counter seam, so the
// perf-probe half is dropped; this keeps the functional guarantee (dst keeps
// its winning scalar, and is independent of src after src is released). A true
// "no wasteful clone" check now needs ASan/LeakSanitizer.
TEST(merge_does_not_clone_when_src_loses_lww) {
    Map *dst = fresh();
    Map *src = fresh();

    // dst has newer scalar at "k".
    map_set(dst, SK("k"), EI(42), stmp(10, 1));

    // src has a nested Counter at "k" with OLDER stamp — must lose LWW.
    Counter *sc = counter_create(default_id());
    for (int i = 0; i < 50; i++) {
        counter_inc(sc, cid((uint8_t)(i + 1)), 1);
    }
    map_set(src, SK("k"), element_counter(sc), stmp(1, 1));
    counter_release(sc);

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(42));

    map_release(src); // dst must be unaffected
    Element out2;
    ASSERT(map_get(dst, SK("k"), &out2) == true);
    ASSERT_SCALAR_EQ(out2, scalar_int(42));
    map_release(dst);
}

TEST(merge_kind_mismatch_clones_winner_into_dst) {
    Map *dst = fresh();
    Counter *dc = counter_create(default_id());
    counter_inc(dc, cid(1), 5);
    map_set(dst, SK("x"), element_counter(dc), stmp(1, 1));
    counter_release(dc);

    Map *src = fresh();
    Register *sr = register_create(default_id(), scalar_int(42), stmp(10, 1));
    map_set(src, SK("x"), element_register(sr), stmp(10, 1));
    register_release(sr);

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("x"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_REGISTER);
    ASSERT(out.as.reg != sr); // clone, not src's pointer
    ASSERT(scalar_eq(register_read(out.as.reg), scalar_int(42)));
    map_release(dst);
    map_release(src);
}

// Two replicas hold a Counter of the same kind at the same slot but with
// DIFFERENT ids. They are two distinct logical elements that happen to
// share a key — typically because the app bypassed the helper and used
// raw counter_create with hand-picked ids. map_merge must NOT recurse
// (which would silently union their tallies); it must take the LWW path
// and orphan one side.
TEST(merge_same_kind_different_id_uses_lww_not_recurse) {
    Map *dst = fresh();
    Map *src = fresh();

    // dst: distinct id, 5 increments under cid 1, older slot stamp.
    Counter *dc = counter_create(eid(7, 1));
    counter_inc(dc, cid(1), 5);
    map_set(dst, SK("votes"), element_counter(dc), stmp(1, 1));
    counter_release(dc);

    // src: DIFFERENT id, 3 increments under cid 2, newer slot stamp.
    Counter *sc = counter_create(eid(7, 2));
    counter_inc(sc, cid(2), 3);
    map_set(src, SK("votes"), element_counter(sc), stmp(5, 1));
    counter_release(sc);

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("votes"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);

    // LWW (src wins on stamp 5 > 1) → dst holds a CLONE of src's Counter,
    // not the unioned recursive-merge result. Recursive would read 8.
    ASSERT_EQ(counter_read(out.as.counter), 3);

    // Clone's id is src's id, not dst's old id (dst's Counter is orphaned).
    ASSERT(elementid_eq(counter_id(out.as.counter), eid(7, 2)) == true);

    // dst owns the clone — not src's pointer.
    ASSERT(out.as.counter != sc);

    map_release(dst);
    map_release(src);
}

// --- get-or-create helpers ---
//
// map_counter / map_register / map_map install a composite at the given key if
// the slot is empty or has a different kind (and the stamp wins LWW). The
// INSTALL path returns a BORROW (the slot owns the ref). If the slot already
// has a matching kind, the existing pointer is returned (stamp + seed ignored).
// If the stamp LOSES LWW, a DETACHED owned handle is returned (born displaced,
// rc=1) and the caller must release it.

TEST(map_counter_creates_and_installs_at_key) {
    Map *m = fresh();

    Counter *c = map_counter(m, SK("votes"), stmp(1, 1));
    ASSERT(c != NULL);

    Element out;
    ASSERT(map_get(m, SK("votes"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT(out.as.counter == c);
    map_release(m); // installed borrow is owned by the slot
}

TEST(map_counter_returns_same_pointer_on_repeat) {
    Map *m = fresh();

    Counter *first = map_counter(m, SK("votes"), stmp(1, 1));
    Counter *second = map_counter(m, SK("votes"), stmp(2, 1));
    ASSERT(first == second);
    map_release(m);
}

TEST(map_register_creates_and_installs_at_key) {
    Map *m = fresh();

    Register *r = map_register(m, SK("title"), scalar_int(42), stmp(1, 1));
    ASSERT(r != NULL);
    ASSERT(scalar_eq(register_read(r), scalar_int(42)));

    Element out;
    ASSERT(map_get(m, SK("title"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_REGISTER);
    ASSERT(out.as.reg == r);
    map_release(m);
}

TEST(map_register_returns_same_pointer_on_repeat) {
    Map *m = fresh();

    Register *first = map_register(m, SK("title"), scalar_int(1), stmp(1, 1));
    // Second call's seed value is ignored — slot already exists.
    Register *second =
        map_register(m, SK("title"), scalar_int(999), stmp(2, 1));
    ASSERT(first == second);
    ASSERT(scalar_eq(register_read(first), scalar_int(1)));
    map_release(m);
}

TEST(map_map_creates_and_installs_at_key) {
    Map *outer = fresh();

    Map *child = map_map(outer, SK("child"), stmp(1, 1));
    ASSERT(child != NULL);

    Element out;
    ASSERT(map_get(outer, SK("child"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_MAP);
    ASSERT(out.as.map == child);
    map_release(outer);
}

TEST(map_map_returns_same_pointer_on_repeat) {
    Map *outer = fresh();

    Map *first = map_map(outer, SK("child"), stmp(1, 1));
    Map *second = map_map(outer, SK("child"), stmp(2, 1));
    ASSERT(first == second);
    map_release(outer);
}

// Helper called over a different-kind slot with a winning stamp flips the kind
// via LWW and returns a fresh installed composite. The displaced Counter is
// released by the Map; a caller that wants to observe it must hold its own ref.
TEST(map_register_after_map_counter_flips_kind_via_lww) {
    Map *m = fresh();

    Counter *c = map_counter(m, SK("score"), stmp(1, 1));
    ASSERT(c != NULL);
    counter_acquire(c); // retain past the imminent eviction

    Register *r = map_register(m, SK("score"), scalar_int(42), stmp(5, 1));
    ASSERT(r != NULL);

    Element out;
    ASSERT(map_get(m, SK("score"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_REGISTER);
    ASSERT(out.as.reg == r);

    // The displaced Counter is still alive via the retained ref, just
    // unreachable from the slot.
    ASSERT(counter_is_displaced(c) == true);
    ASSERT_EQ(counter_read(c), 0);
    counter_release(c);
    map_release(m);
}

// Helper called with a stamp that LOSES LWW returns a DETACHED composite: an
// owned, born-displaced handle. The slot keeps its existing content, and the
// caller must release the detached handle.
TEST(map_helper_losing_stamp_returns_detached_and_keeps_slot) {
    Map *m = fresh();

    Counter *c = map_counter(m, SK("score"), stmp(10, 1));
    ASSERT(c != NULL);

    Register *r = map_register(m, SK("score"), scalar_int(7), stmp(5, 1));
    ASSERT(r != NULL); // detached, but still returned
    ASSERT(scalar_eq(register_read(r), scalar_int(7)));
    ASSERT(register_is_displaced(r) == true); // born displaced

    // Slot kept its Counter — detached Register did not displace it.
    Element out;
    ASSERT(map_get(m, SK("score"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT(out.as.counter == c);

    register_release(r); // caller owns the detached handle
    map_release(m);
}

// map_counter losing LWW returns a detached, born-displaced Counter; the slot
// keeps its existing (different-kind) content.
TEST(map_counter_losing_stamp_returns_detached_displaced) {
    Map *m = fresh();
    map_register(m, SK("score"), scalar_int(1), stmp(10, 1)); // slot-owned

    Counter *c = map_counter(m, SK("score"), stmp(5, 1)); // loses LWW
    ASSERT(c != NULL);
    ASSERT(counter_is_displaced(c) == true);

    Element out;
    ASSERT(map_get(m, SK("score"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_REGISTER); // slot unchanged

    counter_release(c); // caller owns the detached handle
    map_release(m);
}

// map_map losing LWW returns a detached, born-displaced Map.
TEST(map_map_losing_stamp_returns_detached_displaced) {
    Map *m = fresh();
    map_register(m, SK("child"), scalar_int(1), stmp(10, 1));

    Map *child = map_map(m, SK("child"), stmp(5, 1)); // loses LWW
    ASSERT(child != NULL);
    ASSERT(map_is_displaced(child) == true);

    Element out;
    ASSERT(map_get(m, SK("child"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_REGISTER);

    map_release(child); // caller owns the detached handle
    map_release(m);
}

// A helper INSTALL returns a borrow: the slot owns the sole ref. A caller that
// wants to outlive the Map must acquire its own ref; map_release then drops
// only the slot's ref, not the caller's. (Also guards the helper's release
// pairing under ASan — an over-release would surface here as UAF.)
TEST(map_counter_installed_handle_is_a_borrow) {
    Map *m = fresh();
    Counter *c = map_counter(m, SK("votes"), stmp(1, 1)); // borrow, slot owns
    counter_acquire(c);                                   // caller co-owns
    counter_inc(c, cid(1), 4);
    map_release(m); // slot drops its ref; c stays alive on the caller's ref
    ASSERT_EQ(counter_read(c), 4);
    counter_release(c); // last ref → freed
}

// Cross-replica: two replicas each call map_counter on the same key. They get
// separate Counter pointers, but merge takes the recursive path because
// (key, kind, id) matches.
TEST(map_counter_cross_replica_merge_recurses) {
    Map *dst = fresh();
    Map *src = fresh();

    Counter *dc = map_counter(dst, SK("votes"), stmp(1, 1));
    Counter *sc = map_counter(src, SK("votes"), stmp(1, 2));
    counter_inc(dc, cid(1), 5);
    counter_inc(sc, cid(2), 3);

    map_merge(dst, src);

    Element out;
    ASSERT(map_get(dst, SK("votes"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT_EQ(counter_read(out.as.counter), 8);
    map_release(dst);
    map_release(src);
}

// --- helper id derivation ---
//
// Helpers must derive ids deterministically from (parent_id, key, kind) so two
// replicas independently calling the same helper land on the same id.

TEST(map_counter_derives_id_from_parent_key_kind) {
    ElementId parent_id = eid(7, 42);
    Map *m = map_create(parent_id);

    Counter *c = map_counter(m, SK("votes"), stmp(1, 1));
    ElementId expected =
        elementid_derive(parent_id, SK("votes"), (uint8_t)ELEMENT_COUNTER);
    ASSERT(elementid_eq(counter_id(c), expected) == true);
    map_release(m);
}

TEST(map_register_derives_id_from_parent_key_kind) {
    ElementId parent_id = eid(7, 42);
    Map *m = map_create(parent_id);

    Register *r = map_register(m, SK("title"), scalar_int(0), stmp(1, 1));
    ElementId expected =
        elementid_derive(parent_id, SK("title"), (uint8_t)ELEMENT_REGISTER);
    ASSERT(elementid_eq(register_id(r), expected) == true);
    map_release(m);
}

TEST(map_map_derives_id_from_parent_key_kind) {
    ElementId parent_id = eid(7, 42);
    Map *m = map_create(parent_id);

    Map *child = map_map(m, SK("child"), stmp(1, 1));
    ElementId expected =
        elementid_derive(parent_id, SK("child"), (uint8_t)ELEMENT_MAP);
    ASSERT(elementid_eq(map_id(child), expected) == true);
    map_release(m);
}

// Two replicas with the same parent_id calling the same helper at the same key
// land on identical ids — the convergent-creation guarantee.
TEST(helpers_converge_across_replicas) {
    ElementId shared_parent = eid(7, 42);
    Map *map_a = map_create(shared_parent);
    Map *map_b = map_create(shared_parent);

    Counter *ca = map_counter(map_a, SK("votes"), stmp(1, 1));
    Counter *cb = map_counter(map_b, SK("votes"), stmp(1, 2));

    ASSERT(elementid_eq(counter_id(ca), counter_id(cb)) == true);
    map_release(map_a);
    map_release(map_b);
}

// Different kinds at the same key derive DIFFERENT ids — that's how recursive
// merge distinguishes Counter@"x" from Register@"x" as independent elements.
TEST(helpers_at_same_key_different_kind_have_distinct_ids) {
    Map *m = map_create(eid(7, 42));

    Counter *c = map_counter(m, SK("x"), stmp(1, 1));
    // Counter is installed. map_register loses the LWW slot here (same stamp),
    // so returns a DETACHED Register. Its id should still be derived from
    // (parent_id, "x", REGISTER), distinct from c's id.
    Register *r = map_register(m, SK("x"), scalar_int(0), stmp(1, 1));
    ASSERT(elementid_eq(counter_id(c), register_id(r)) == false);
    register_release(r); // detached owned handle
    map_release(m);
}

// --- map lifecycle: release / displace ---

// map_release drops the Map's ref on each live slot composite (recursively for
// nested maps). A composite the caller also holds a ref on survives until the
// caller releases too.
TEST(map_release_drops_slot_refs_but_held_ref_survives) {
    Map *m = fresh();
    Counter *c = counter_create(default_id());
    counter_inc(c, cid(1), 7);
    map_set(m, SK("votes"), element_counter(c), stmp(1, 1));
    // Caller keeps its ref (refcount = 2: caller + Map).

    map_release(m); // drops Map's ref → refcount = 1, c still alive

    ASSERT_EQ(counter_read(c), 7);
    counter_release(c); // last ref → freed
}

// map_displace forwards to the Map's own displaced flag (Map is itself a
// composite kind that can be displaced from a parent slot).
TEST(map_displace_sets_flag) {
    Map *m = fresh();
    ASSERT(map_is_displaced(m) == false);
    map_displace(m);
    ASSERT(map_is_displaced(m) == true);
    map_release(m);
}

// --- map_clone: deep recursive copy, refcount=1 ---

TEST(clone_empty_map_is_empty) {
    Map *src = fresh();
    Map *clone = map_clone(src);
    ASSERT(clone != NULL);
    ASSERT(clone != src);
    ASSERT_EQ(map_size(clone), 0);
    map_release(src);
    map_release(clone);
}

TEST(clone_preserves_scalar_slots) {
    Map *src = fresh();
    map_set(src, SK("a"), EI(1), stmp(1, 1));
    map_set(src, SK("b"), ES("hi", 2), stmp(1, 1));
    Map *clone = map_clone(src);
    ASSERT_EQ(map_size(clone), 2);
    Element a_out, b_out;
    ASSERT(map_get(clone, SK("a"), &a_out) == true);
    ASSERT(map_get(clone, SK("b"), &b_out) == true);
    ASSERT_SCALAR_EQ(a_out, scalar_int(1));
    ASSERT_SCALAR_EQ(b_out, scalar_string((const uint8_t *)"hi", 2));
    map_release(src);
    map_release(clone);
}

// Clone owns all its data — releasing the source leaves the clone fully usable.
TEST(clone_survives_src_release) {
    Map *src = fresh();
    map_set(src, SK("k"), ES("hello", 5), stmp(1, 1));
    Map *clone = map_clone(src);
    map_release(src);
    Element out;
    ASSERT(map_get(clone, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_string((const uint8_t *)"hello", 5));
    map_release(clone);
}

// Composite slots are recursively cloned — the clone's nested composites are
// independent objects with their own refcounts.
TEST(clone_recurses_into_composite_slots) {
    Map *src = fresh();
    Counter *sc = counter_create(default_id());
    counter_inc(sc, cid(1), 5);
    map_set(src, SK("votes"), element_counter(sc), stmp(1, 1));
    counter_release(sc);

    Map *clone = map_clone(src);

    Element out;
    ASSERT(map_get(clone, SK("votes"), &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_COUNTER);
    ASSERT(out.as.counter != sc); // recursive clone, independent object
    ASSERT_EQ(counter_read(out.as.counter), 5);

    map_release(src);
    Element out2;
    ASSERT(map_get(clone, SK("votes"), &out2) == true);
    ASSERT_EQ(counter_read(out2.as.counter), 5);
    map_release(clone);
}

// Tombstones must round-trip through clone so deletion semantics survive.
TEST(clone_preserves_tombstones) {
    Map *src = fresh();
    map_set(src, SK("k"), EI(1), stmp(1, 1));
    map_delete(src, SK("k"), stmp(5, 1));

    Map *clone = map_clone(src);

    // Tombstone present at stamp(5,1) — older set must lose LWW.
    map_set(clone, SK("k"), EI(99), stmp(3, 1));
    Element out;
    ASSERT(map_get(clone, SK("k"), &out) == false);
    map_release(src);
    map_release(clone);
}

// Mutating src after clone must not affect the clone.
TEST(clone_independent_of_src) {
    Map *src = fresh();
    map_set(src, SK("k"), EI(1), stmp(1, 1));
    Map *clone = map_clone(src);
    map_set(src, SK("k"), EI(99), stmp(5, 1));
    map_set(src, SK("new"), EI(7), stmp(1, 1));

    Element out;
    ASSERT(map_get(clone, SK("k"), &out) == true);
    ASSERT_SCALAR_EQ(out, scalar_int(1));
    ASSERT(map_get(clone, SK("new"), &out) == false);
    map_release(src);
    map_release(clone);
}

// Tombstone entries carry a stale value field. map_clone must NOT recursively
// clone that stale value into the destination.
//
// NOTE: the original arena-based test measured this via arena_used byte cost.
// Without an alloc-counter seam there is no refcount equivalent, so the
// perf-probe half is dropped; this keeps the functional guarantee (tombstone
// survives the clone, clone is independent). A real "did not recurse the stale
// value" check now needs ASan/LeakSanitizer.
TEST(clone_tombstone_does_not_recurse_into_stale_value) {
    Map *src = fresh();

    // Inner map under the to-be-tombstoned key.
    Map *inner = map_create(default_id());
    for (int i = 0; i < 10; i++) {
        char k[16];
        int n = snprintf(k, sizeof k, "k%d", i);
        map_set(inner, k, (size_t)n, EI(i), stmp(1, 1));
    }
    map_set(src, SK("child"), element_map(inner), stmp(1, 1));
    map_release(inner);
    // Delete — Entry stays in src with is_tombstone=true but its value field
    // may still reference the (now released-by-map) inner subtree.
    map_delete(src, SK("child"), stmp(5, 1));

    Map *clone = map_clone(src);

    // Tombstone semantics survive.
    Element out;
    ASSERT(map_get(clone, SK("child"), &out) == false);

    map_release(src);
    map_release(clone);
}

int main(void) {
    RUN(map_create_stores_id);
    RUN(empty_get_returns_false);
    RUN(set_then_get);
    RUN(set_overwrites_with_newer_stamp);
    RUN(set_lower_stamp_ignored);
    RUN(set_equal_lamport_higher_client_wins);
    RUN(set_equal_lamport_lower_client_ignored);
    RUN(set_same_stamp_idempotent);
    RUN(set_can_change_value_kind);
    RUN(distinct_keys_are_independent);
    RUN(keys_with_embedded_nul_are_distinct);

    RUN(delete_makes_get_return_false);
    RUN(delete_with_lower_stamp_ignored);
    RUN(set_after_delete_with_higher_stamp_resurrects);
    RUN(set_after_delete_with_lower_stamp_ignored);
    RUN(set_vs_delete_higher_stamp_wins_delete);
    RUN(delete_idempotent_same_stamp);
    RUN(delete_absent_key_still_installs_tombstone);

    RUN(size_zero_initially);
    RUN(size_counts_live_entries);
    RUN(size_excludes_tombstones);
    RUN(size_recovers_on_resurrect);

    RUN(set_counter_then_get_returns_element_counter);
    RUN(set_register_then_get_returns_element_register);
    RUN(set_nested_map_then_get_returns_element_map);

    RUN(merge_disjoint_keys_unions);
    RUN(merge_same_key_newer_wins);
    RUN(merge_src_older_loses);
    RUN(merge_delete_beats_older_set);
    RUN(merge_set_beats_older_delete);
    RUN(merge_commutative);
    RUN(merge_idempotent);
    RUN(merge_associative);
    RUN(merge_does_not_mutate_src);
    RUN(merge_copies_string_into_dst);
    RUN(merge_preserves_tombstone_against_older_set);

    RUN(merge_same_kind_counter_recurses);
    RUN(merge_same_kind_register_recurses);
    RUN(merge_same_kind_nested_map_recurses);
    RUN(merge_same_kind_counter_does_not_mutate_src);
    RUN(merge_same_kind_counter_advances_slot_stamp);

    RUN(set_composite_displaces_scalar_at_lww);
    RUN(set_scalar_displaces_composite_at_lww);
    RUN(set_different_kind_composite_displaces_at_lww);
    RUN(evicted_composite_is_displaced_and_outlives_via_held_ref);
    RUN(delete_composite_displaces_it);

    RUN(merge_composite_src_wins_into_empty_slot_clones);
    RUN(merge_does_not_clone_when_src_loses_lww);
    RUN(merge_kind_mismatch_clones_winner_into_dst);
    RUN(merge_same_kind_different_id_uses_lww_not_recurse);

    RUN(map_counter_creates_and_installs_at_key);
    RUN(map_counter_returns_same_pointer_on_repeat);
    RUN(map_register_creates_and_installs_at_key);
    RUN(map_register_returns_same_pointer_on_repeat);
    RUN(map_map_creates_and_installs_at_key);
    RUN(map_map_returns_same_pointer_on_repeat);
    RUN(map_register_after_map_counter_flips_kind_via_lww);
    RUN(map_helper_losing_stamp_returns_detached_and_keeps_slot);
    RUN(map_counter_losing_stamp_returns_detached_displaced);
    RUN(map_map_losing_stamp_returns_detached_displaced);
    RUN(map_counter_installed_handle_is_a_borrow);
    RUN(map_counter_cross_replica_merge_recurses);

    RUN(map_counter_derives_id_from_parent_key_kind);
    RUN(map_register_derives_id_from_parent_key_kind);
    RUN(map_map_derives_id_from_parent_key_kind);
    RUN(helpers_converge_across_replicas);
    RUN(helpers_at_same_key_different_kind_have_distinct_ids);

    RUN(map_release_drops_slot_refs_but_held_ref_survives);
    RUN(map_displace_sets_flag);

    RUN(clone_empty_map_is_empty);
    RUN(clone_preserves_scalar_slots);
    RUN(clone_survives_src_release);
    RUN(clone_recurses_into_composite_slots);
    RUN(clone_preserves_tombstones);
    RUN(clone_independent_of_src);
    RUN(clone_tombstone_does_not_recurse_into_stale_value);

    TEST_SUMMARY();
}
