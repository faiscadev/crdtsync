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

// element_release on a SCALAR frees the scalar's bytes, so it is valid ONLY on
// owned scalars (those produced by element_clone, i.e. scalar_clone provenance)
// — never on a borrowed-buffer element_scalar(...) passed transiently into
// map_set.

// Helpers.

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

static ElementId default_id(void) { return eid(0xFF, 0); }

static Stamp stmp(uint64_t lamport, uint8_t client_first_byte) {
    return (Stamp){.lamport = lamport, .client_id = cid(client_first_byte)};
}

// --- constructors set kind + payload ---

TEST(scalar_constructor_sets_kind_and_value) {
    Element e = element_scalar(scalar_int(42));
    ASSERT_EQ(element_kind(e), ELEMENT_SCALAR);
    ASSERT(scalar_eq(e.as.scalar, scalar_int(42)));
}

TEST(register_constructor_sets_kind_and_pointer) {
    Register *r = register_create(default_id(), scalar_int(1), stmp(1, 1));
    Element e = element_register(r);
    ASSERT_EQ(element_kind(e), ELEMENT_REGISTER);
    ASSERT(e.as.reg == r);
    element_release(e);
}

TEST(counter_constructor_sets_kind_and_pointer) {
    Counter *c = counter_create(default_id());
    Element e = element_counter(c);
    ASSERT_EQ(element_kind(e), ELEMENT_COUNTER);
    ASSERT(e.as.counter == c);
    element_release(e);
}

TEST(map_constructor_sets_kind_and_pointer) {
    Map *m = map_create(default_id());
    Element e = element_map(m);
    ASSERT_EQ(element_kind(e), ELEMENT_MAP);
    ASSERT(e.as.map == m);
    element_release(e);
}

// --- kind name (for diagnostics) ---

TEST(kind_name_scalar) {
    ASSERT(strcmp(element_kind_name(ELEMENT_SCALAR), "SCALAR") == 0);
}

TEST(kind_name_register) {
    ASSERT(strcmp(element_kind_name(ELEMENT_REGISTER), "REGISTER") == 0);
}

TEST(kind_name_counter) {
    ASSERT(strcmp(element_kind_name(ELEMENT_COUNTER), "COUNTER") == 0);
}

TEST(kind_name_map) {
    ASSERT(strcmp(element_kind_name(ELEMENT_MAP), "MAP") == 0);
}

// --- element_id reads the composite's id ---

TEST(id_register) {
    ElementId id = eid(7, 42);
    Register *r = register_create(id, scalar_int(1), stmp(1, 1));
    ASSERT(elementid_eq(element_id(element_register(r)), id) == true);
    register_release(r);
}

TEST(id_counter) {
    ElementId id = eid(7, 42);
    Counter *c = counter_create(id);
    ASSERT(elementid_eq(element_id(element_counter(c)), id) == true);
    counter_release(c);
}

TEST(id_map) {
    ElementId id = eid(7, 42);
    Map *m = map_create(id);
    ASSERT(elementid_eq(element_id(element_map(m)), id) == true);
    map_release(m);
}

// --- merge dispatches by kind to the underlying _merge ---

TEST(merge_register_takes_newer_value) {
    Register *dst = register_create(default_id(), scalar_int(10), stmp(1, 1));
    Register *src = register_create(default_id(), scalar_int(20), stmp(5, 1));

    element_merge(element_register(dst), element_register(src));

    ASSERT(scalar_eq(register_read(dst), scalar_int(20)));
    register_release(dst);
    register_release(src);
}

TEST(merge_counter_unions_clients) {
    Counter *dst = counter_create(default_id());
    Counter *src = counter_create(default_id());
    counter_inc(dst, cid(1), 5);
    counter_inc(src, cid(2), 3);

    element_merge(element_counter(dst), element_counter(src));

    ASSERT_EQ(counter_read(dst), 8);
    counter_release(dst);
    counter_release(src);
}

TEST(merge_map_takes_newer_slot) {
    Map *dst = map_create(default_id());
    Map *src = map_create(default_id());

    const uint8_t *k = (const uint8_t *)"k";
    size_t klen = 1;
    map_set(dst, k, klen, element_scalar(scalar_int(10)), stmp(1, 1));
    map_set(src, k, klen, element_scalar(scalar_int(20)), stmp(5, 1));

    element_merge(element_map(dst), element_map(src));

    Element out;
    ASSERT(map_get(dst, k, klen, &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_SCALAR);
    ASSERT(scalar_eq(out.as.scalar, scalar_int(20)));
    map_release(dst);
    map_release(src);
}

TEST(merge_register_does_not_mutate_src) {
    Register *dst = register_create(default_id(), scalar_int(99), stmp(10, 1));
    Register *src = register_create(default_id(), scalar_int(7), stmp(1, 1));

    element_merge(element_register(dst), element_register(src));

    ASSERT(scalar_eq(register_read(src), scalar_int(7)));
    register_release(dst);
    register_release(src);
}

TEST(merge_counter_does_not_mutate_src) {
    Counter *dst = counter_create(default_id());
    Counter *src = counter_create(default_id());
    counter_inc(src, cid(1), 3);

    element_merge(element_counter(dst), element_counter(src));

    ASSERT_EQ(counter_read(src), 3);
    counter_release(dst);
    counter_release(src);
}

TEST(merge_map_does_not_mutate_src) {
    Map *dst = map_create(default_id());
    Map *src = map_create(default_id());
    const uint8_t *k = (const uint8_t *)"k";
    map_set(src, k, 1, element_scalar(scalar_int(7)), stmp(1, 1));

    element_merge(element_map(dst), element_map(src));

    Element out;
    ASSERT(map_get(src, k, 1, &out) == true);
    ASSERT_EQ(element_kind(out), ELEMENT_SCALAR);
    ASSERT(scalar_eq(out.as.scalar, scalar_int(7)));
    map_release(dst);
    map_release(src);
}

TEST(round_trip_via_kind_and_payload) {
    Counter *c = counter_create(default_id());
    Element e = element_counter(c);
    ASSERT_EQ(element_kind(e), ELEMENT_COUNTER);
    ASSERT(e.as.counter == c);
    element_release(e);
}

// --- element_clone: deep copy, refcount=1 children ---
//
// Used by map_merge when an LWW winner is a composite from a foreign replica.
// The clone owns all its memory; releasing the source must leave the clone
// intact. Mutating the source after clone must NOT affect the clone.

TEST(clone_scalar_int_preserves_value) {
    Element clone = element_clone(element_scalar(scalar_int(42)));
    ASSERT_EQ(element_kind(clone), ELEMENT_SCALAR);
    ASSERT(scalar_eq(clone.as.scalar, scalar_int(42)));
    element_release(clone); // owned scalar (int) — scalar_free is a no-op
}

// Clone owns its string bytes: scribbling the source buffer after clone must
// not change what the clone reads.
TEST(clone_scalar_string_owns_bytes) {
    uint8_t buf[8];
    memcpy(buf, "hello", 5);
    Element clone = element_clone(element_scalar(scalar_string(buf, 5)));
    buf[0] = 'X';
    buf[1] = 'X';
    ASSERT_EQ(element_kind(clone), ELEMENT_SCALAR);
    ASSERT(
        scalar_eq(clone.as.scalar, scalar_string((const uint8_t *)"hello", 5)));
    element_release(clone); // owned string — frees the host_malloc'd copy
}

TEST(clone_register_deep_copies_value) {
    Register *src = register_create(default_id(), scalar_int(42), stmp(5, 1));
    Element clone = element_clone(element_register(src));
    register_release(src); // src frees; clone must survive
    ASSERT_EQ(element_kind(clone), ELEMENT_REGISTER);
    ASSERT(clone.as.reg != src);
    ASSERT(scalar_eq(register_read(clone.as.reg), scalar_int(42)));
    element_release(clone);
}

TEST(clone_counter_deep_copies_per_client_tallies) {
    Counter *src = counter_create(default_id());
    counter_inc(src, cid(1), 5);
    counter_inc(src, cid(2), 3);
    Element clone = element_clone(element_counter(src));
    counter_release(src);
    ASSERT_EQ(element_kind(clone), ELEMENT_COUNTER);
    ASSERT(clone.as.counter != src);
    ASSERT_EQ(counter_read(clone.as.counter), 8);
    element_release(clone);
}

TEST(clone_map_deep_copies_recursively) {
    Map *src = map_create(default_id());
    map_set(src, (const void *)"a", 1, element_scalar(scalar_int(1)),
            stmp(1, 1));
    map_set(src, (const void *)"b", 1, element_scalar(scalar_int(2)),
            stmp(1, 1));
    Element clone = element_clone(element_map(src));
    map_release(src);
    ASSERT_EQ(element_kind(clone), ELEMENT_MAP);
    ASSERT(clone.as.map != src);
    Element a_out, b_out;
    ASSERT(map_get(clone.as.map, (const void *)"a", 1, &a_out) == true);
    ASSERT(map_get(clone.as.map, (const void *)"b", 1, &b_out) == true);
    ASSERT(scalar_eq(a_out.as.scalar, scalar_int(1)));
    ASSERT(scalar_eq(b_out.as.scalar, scalar_int(2)));
    element_release(clone);
}

// Mutating src after clone must not affect the clone.
TEST(clone_counter_independent_of_src) {
    Counter *src = counter_create(default_id());
    counter_inc(src, cid(1), 5);
    Element clone = element_clone(element_counter(src));
    counter_inc(src, cid(1), 100);
    ASSERT_EQ(counter_read(src), 105);
    ASSERT_EQ(counter_read(clone.as.counter), 5);
    counter_release(src);
    element_release(clone);
}

// --- element_clone preserves the source's id ---

TEST(clone_register_preserves_id) {
    ElementId id = eid(7, 42);
    Register *src = register_create(id, scalar_int(1), stmp(1, 1));
    Element clone = element_clone(element_register(src));
    ASSERT_EQ(element_kind(clone), ELEMENT_REGISTER);
    ASSERT(elementid_eq(register_id(clone.as.reg), id) == true);
    register_release(src);
    element_release(clone);
}

TEST(clone_counter_preserves_id) {
    ElementId id = eid(7, 42);
    Counter *src = counter_create(id);
    Element clone = element_clone(element_counter(src));
    ASSERT_EQ(element_kind(clone), ELEMENT_COUNTER);
    ASSERT(elementid_eq(counter_id(clone.as.counter), id) == true);
    counter_release(src);
    element_release(clone);
}

TEST(clone_map_preserves_id) {
    ElementId id = eid(7, 42);
    Map *src = map_create(id);
    Element clone = element_clone(element_map(src));
    ASSERT_EQ(element_kind(clone), ELEMENT_MAP);
    ASSERT(elementid_eq(map_id(clone.as.map), id) == true);
    map_release(src);
    element_release(clone);
}

// --- lifecycle forwarding: acquire / release / displace / is_displaced ---
//
// Element forwards each call to the underlying composite. For SCALAR:
// acquire/displace are no-ops, is_displaced is always false, release frees
// owned bytes.

// element_acquire bumps the composite refcount; the matching element_release
// drops it. One extra acquire + one extra release keeps the composite alive.
TEST(acquire_release_balanced_keeps_composite_alive) {
    Counter *c = counter_create(default_id());
    counter_inc(c, cid(1), 5);
    Element e = element_counter(c);
    element_acquire(e); // refcount 1 -> 2
    element_release(e); // refcount 2 -> 1, still alive
    ASSERT_EQ(counter_read(c), 5);
    element_release(e); // refcount 1 -> 0, freed
}

// element_displace forwards to the composite's displace flag.
TEST(displace_forwards_to_composite) {
    Counter *c = counter_create(default_id());
    Element e = element_counter(c);
    ASSERT(element_is_displaced(e) == false);
    element_displace(e);
    ASSERT(element_is_displaced(e) == true);
    ASSERT(counter_is_displaced(c) == true); // forwarded to the real flag
    element_release(e);
}

TEST(displace_forwards_to_register) {
    Register *r = register_create(default_id(), scalar_int(1), stmp(1, 1));
    Element e = element_register(r);
    element_displace(e);
    ASSERT(register_is_displaced(r) == true);
    element_release(e);
}

// SCALAR has no composite: displace is a no-op, is_displaced is always false.
TEST(scalar_displace_is_noop) {
    Element e = element_scalar(scalar_int(7));
    element_displace(e); // must not crash
    ASSERT(element_is_displaced(e) == false);
    // No element_release: a borrowed-int scalar owns nothing.
}

// element_acquire on a SCALAR is a no-op (nothing to refcount); must not crash.
TEST(scalar_acquire_is_noop) {
    Element e = element_scalar(scalar_int(7));
    element_acquire(e);
    ASSERT(element_is_displaced(e) == false);
}

// Clone of a displaced composite is itself not displaced — displacement is a
// per-instance signal, reset on clone (verified at the primitive level too).
TEST(clone_of_displaced_composite_is_not_displaced) {
    Counter *src = counter_create(default_id());
    element_displace(element_counter(src));
    Element clone = element_clone(element_counter(src));
    ASSERT(element_is_displaced(clone) == false);
    counter_release(src);
    element_release(clone);
}

int main(void) {
    RUN(scalar_constructor_sets_kind_and_value);
    RUN(register_constructor_sets_kind_and_pointer);
    RUN(counter_constructor_sets_kind_and_pointer);
    RUN(map_constructor_sets_kind_and_pointer);

    RUN(kind_name_scalar);
    RUN(kind_name_register);
    RUN(kind_name_counter);
    RUN(kind_name_map);

    RUN(id_register);
    RUN(id_counter);
    RUN(id_map);

    RUN(merge_register_takes_newer_value);
    RUN(merge_counter_unions_clients);
    RUN(merge_map_takes_newer_slot);

    RUN(merge_register_does_not_mutate_src);
    RUN(merge_counter_does_not_mutate_src);
    RUN(merge_map_does_not_mutate_src);

    RUN(round_trip_via_kind_and_payload);

    RUN(clone_scalar_int_preserves_value);
    RUN(clone_scalar_string_owns_bytes);
    RUN(clone_register_deep_copies_value);
    RUN(clone_counter_deep_copies_per_client_tallies);
    RUN(clone_map_deep_copies_recursively);
    RUN(clone_counter_independent_of_src);

    RUN(clone_register_preserves_id);
    RUN(clone_counter_preserves_id);
    RUN(clone_map_preserves_id);

    RUN(acquire_release_balanced_keeps_composite_alive);
    RUN(displace_forwards_to_composite);
    RUN(displace_forwards_to_register);
    RUN(scalar_displace_is_noop);
    RUN(scalar_acquire_is_noop);
    RUN(clone_of_displaced_composite_is_not_displaced);

    TEST_SUMMARY();
}
