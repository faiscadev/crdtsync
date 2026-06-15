#include <assert.h>
#include <stdint.h>
#include <string.h>

#include "arena.h"
#include "hashtable.h"
#include "test_util.h"

// hashtable keys on raw bytes (key pointer + length), not C strings.
// Expected API (you implement in hashtable.h / hashtable.c):
//   HashTableInsertResult hashtable_insert(HashTable*, const void *key, size_t
//   key_len, void *value); bool                  hashtable_get   (HashTable*,
//   const void *key, size_t key_len, void **out); HashTableRemoveResult
//   hashtable_remove(HashTable*, const void *key, size_t key_len);
//   HashTableUpdateResult hashtable_update(HashTable*, const void *key, size_t
//   key_len, void *value); HashTableUpsertResult hashtable_upsert(HashTable*,
//   const void *key, size_t key_len, void *value); bool
//   hashtable_iter_next(HashTableIter*, const void **key, size_t *key_len, void
//   **value);
// Keys are copied (key_len bytes) into the arena. Binary-safe: embedded NUL
// bytes are part of the key, and length is significant.

// String-key shorthand: expands to (bytes, length) without the NUL terminator.
#define SK(s) (s), strlen(s)

static HashTable *fresh(void) {
    Arena *arena = arena_create();
    HashTable *table = hashtable_create(arena);
    assert(table != NULL);
    return table;
}

TEST(create_empty) {
    HashTable *t = fresh();

    uint32_t k = 7;
    void *out = (void *)0xdead;
    ASSERT(hashtable_get(t, &k, sizeof k, &out) == false);
    // out must be untouched on miss.
    ASSERT(out == (void *)0xdead);
}

// uint32 key, fetched via a separate variable holding the same value — proves
// the table compares key *bytes*, not pointers.
TEST(insert_then_get) {
    HashTable *t = fresh();

    uint32_t k = 42;
    int v = 99;
    ASSERT_EQ(hashtable_insert(t, &k, sizeof k, &v), HASHTABLE_OK);

    uint32_t k2 = 42;
    void *out = NULL;
    ASSERT(hashtable_get(t, &k2, sizeof k2, &out) == true);
    ASSERT(out == &v);
    ASSERT_EQ(*(int *)out, 99);
}

TEST(insert_duplicate_rejected) {
    HashTable *t = fresh();

    int a = 1, b = 2;
    ASSERT_EQ(hashtable_insert(t, SK("k"), &a), HASHTABLE_OK);
    ASSERT_EQ(hashtable_insert(t, SK("k"), &b), HASHTABLE_ERR_KEY_EXISTS);

    void *out = NULL;
    ASSERT(hashtable_get(t, SK("k"), &out) == true);
    ASSERT(out == &a);
}

TEST(get_missing_returns_false) {
    HashTable *t = fresh();

    int v = 7;
    hashtable_insert(t, SK("present"), &v);

    void *out = NULL;
    ASSERT(hashtable_get(t, SK("absent"), &out) == false);
}

// NULL is a storable value, distinguishable from "not found".
TEST(stored_null_is_distinguishable) {
    HashTable *t = fresh();

    ASSERT_EQ(hashtable_insert(t, SK("nullval"), NULL), HASHTABLE_OK);

    void *out = (void *)0xbeef;
    ASSERT(hashtable_get(t, SK("nullval"), &out) == true);
    ASSERT(out == NULL);

    out = (void *)0xbeef;
    ASSERT(hashtable_get(t, SK("other"), &out) == false);
}

TEST(update_existing) {
    HashTable *t = fresh();

    int a = 1, b = 2;
    hashtable_insert(t, SK("k"), &a);
    ASSERT_EQ(hashtable_update(t, SK("k"), &b), HASHTABLE_UPDATE_OK);

    void *out = NULL;
    ASSERT(hashtable_get(t, SK("k"), &out) == true);
    ASSERT(out == &b);
}

TEST(update_missing_rejected) {
    HashTable *t = fresh();

    int b = 2;
    ASSERT_EQ(hashtable_update(t, SK("ghost"), &b),
              HASHTABLE_UPDATE_ERR_NOT_FOUND);
}

TEST(upsert_inserts_when_absent) {
    HashTable *t = fresh();

    int v = 5;
    ASSERT_EQ(hashtable_upsert(t, SK("k"), &v), HASHTABLE_UPSERT_INSERTED);

    void *out = NULL;
    ASSERT(hashtable_get(t, SK("k"), &out) == true);
    ASSERT(out == &v);
}

TEST(upsert_updates_when_present) {
    HashTable *t = fresh();

    int a = 1, b = 2;
    ASSERT_EQ(hashtable_upsert(t, SK("k"), &a), HASHTABLE_UPSERT_INSERTED);
    ASSERT_EQ(hashtable_upsert(t, SK("k"), &b), HASHTABLE_UPSERT_UPDATED);

    void *out = NULL;
    ASSERT(hashtable_get(t, SK("k"), &out) == true);
    ASSERT(out == &b);
}

TEST(remove_existing) {
    HashTable *t = fresh();

    int v = 9;
    hashtable_insert(t, SK("k"), &v);
    ASSERT_EQ(hashtable_remove(t, SK("k")), HASHTABLE_REMOVE_OK);

    void *out = NULL;
    ASSERT(hashtable_get(t, SK("k"), &out) == false);
}

TEST(remove_missing_rejected) {
    HashTable *t = fresh();

    ASSERT_EQ(hashtable_remove(t, SK("nope")), HASHTABLE_REMOVE_ERR_NOT_FOUND);
}

TEST(remove_then_reinsert) {
    HashTable *t = fresh();

    int a = 1, b = 2;
    hashtable_insert(t, SK("k"), &a);
    hashtable_remove(t, SK("k"));
    ASSERT_EQ(hashtable_insert(t, SK("k"), &b), HASHTABLE_OK);

    void *out = NULL;
    ASSERT(hashtable_get(t, SK("k"), &out) == true);
    ASSERT(out == &b);
}

// Table must copy the key bytes: mutating the caller's buffer after insert
// must not corrupt or relocate the stored entry.
TEST(key_is_copied_not_borrowed) {
    HashTable *t = fresh();

    uint8_t key[4] = {1, 2, 3, 4};
    int v = 123;
    hashtable_insert(t, key, sizeof key, &v);

    // Scribble over the caller's buffer.
    key[0] = 9;
    key[1] = 9;

    uint8_t orig[4] = {1, 2, 3, 4};
    void *out = NULL;
    ASSERT(hashtable_get(t, orig, sizeof orig, &out) == true);
    ASSERT(out == &v);

    uint8_t mutated[4] = {9, 9, 3, 4};
    out = NULL;
    ASSERT(hashtable_get(t, mutated, sizeof mutated, &out) == false);
}

// The headline reason for byte keys: keys with embedded NUL bytes must be
// distinguished past the NUL. A string-keyed table would treat all of these
// as "\x01" and collapse them.
TEST(embedded_nul_keys_distinct) {
    HashTable *t = fresh();

    uint8_t k1[3] = {0x01, 0x00, 0x02};
    uint8_t k2[3] = {0x01, 0x00, 0x03};
    uint8_t k3[1] = {0x01};
    int va = 1, vb = 2, vc = 3;

    ASSERT_EQ(hashtable_insert(t, k1, sizeof k1, &va), HASHTABLE_OK);
    ASSERT_EQ(hashtable_insert(t, k2, sizeof k2, &vb), HASHTABLE_OK);
    ASSERT_EQ(hashtable_insert(t, k3, sizeof k3, &vc), HASHTABLE_OK);

    void *out = NULL;
    ASSERT(hashtable_get(t, k1, sizeof k1, &out) == true);
    ASSERT(out == &va);
    ASSERT(hashtable_get(t, k2, sizeof k2, &out) == true);
    ASSERT(out == &vb);
    ASSERT(hashtable_get(t, k3, sizeof k3, &out) == true);
    ASSERT(out == &vc);
}

// Same prefix, different length: must be distinct keys.
TEST(length_distinguishes_keys) {
    HashTable *t = fresh();

    uint8_t a[2] = {0x01, 0x02};
    uint8_t b[3] = {0x01, 0x02, 0x03};
    int va = 1, vb = 2;

    ASSERT_EQ(hashtable_insert(t, a, sizeof a, &va), HASHTABLE_OK);
    ASSERT_EQ(hashtable_insert(t, b, sizeof b, &vb), HASHTABLE_OK);

    void *out = NULL;
    ASSERT(hashtable_get(t, a, sizeof a, &out) == true);
    ASSERT(out == &va);
    ASSERT(hashtable_get(t, b, sizeof b, &out) == true);
    ASSERT(out == &vb);
}

TEST(collisions_resolve) {
    HashTable *t = fresh();

    int va = 1, vb = 2, vc = 3, vd = 4;
    ASSERT_EQ(hashtable_insert(t, SK("alpha"), &va), HASHTABLE_OK);
    ASSERT_EQ(hashtable_insert(t, SK("bravo"), &vb), HASHTABLE_OK);
    ASSERT_EQ(hashtable_insert(t, SK("charlie"), &vc), HASHTABLE_OK);
    ASSERT_EQ(hashtable_insert(t, SK("delta"), &vd), HASHTABLE_OK);

    void *out = NULL;
    ASSERT(hashtable_get(t, SK("alpha"), &out) == true);
    ASSERT(out == &va);
    ASSERT(hashtable_get(t, SK("bravo"), &out) == true);
    ASSERT(out == &vb);
    ASSERT(hashtable_get(t, SK("charlie"), &out) == true);
    ASSERT(out == &vc);
    ASSERT(hashtable_get(t, SK("delta"), &out) == true);
    ASSERT(out == &vd);
}

// Insert many more than initial size to force at least one grow/rehash.
// uint32 keys exercise the byte-key path; all entries must survive the rehash.
TEST(grow_preserves_entries) {
    HashTable *t = fresh();

    enum { N = 200 };
    static uint32_t keys[N];
    static int vals[N];
    for (int i = 0; i < N; i++) {
        keys[i] = (uint32_t)(i * 7 + 1);
        vals[i] = i * 10;
        ASSERT_EQ(hashtable_insert(t, &keys[i], sizeof keys[i], &vals[i]),
                  HASHTABLE_OK);
    }

    for (int i = 0; i < N; i++) {
        void *out = NULL;
        ASSERT(hashtable_get(t, &keys[i], sizeof keys[i], &out) == true);
        ASSERT(out == &vals[i]);
    }
}

TEST(iter_empty_yields_nothing) {
    HashTable *t = fresh();

    HashTableIter it = hashtable_iter(t);
    const void *k = NULL;
    size_t klen = 0;
    void *v = NULL;
    ASSERT(hashtable_iter_next(&it, &k, &klen, &v) == false);
}

// Iteration must visit every entry exactly once (order unspecified), yielding
// the key bytes and their length.
TEST(iter_visits_all_once) {
    HashTable *t = fresh();

    uint32_t k1 = 10, k2 = 20, k3 = 30;
    int v1 = 1, v2 = 2, v3 = 3;
    hashtable_insert(t, &k1, sizeof k1, &v1);
    hashtable_insert(t, &k2, sizeof k2, &v2);
    hashtable_insert(t, &k3, sizeof k3, &v3);

    int seen1 = 0, seen2 = 0, seen3 = 0, total = 0;

    HashTableIter it = hashtable_iter(t);
    const void *k = NULL;
    size_t klen = 0;
    void *v = NULL;
    while (hashtable_iter_next(&it, &k, &klen, &v)) {
        total++;
        ASSERT_EQ(klen, sizeof(uint32_t));
        uint32_t kv;
        memcpy(&kv, k, sizeof kv);
        if (kv == 10) {
            seen1++;
            ASSERT(v == &v1);
        }
        if (kv == 20) {
            seen2++;
            ASSERT(v == &v2);
        }
        if (kv == 30) {
            seen3++;
            ASSERT(v == &v3);
        }
    }

    ASSERT_EQ(total, 3);
    ASSERT_EQ(seen1, 1);
    ASSERT_EQ(seen2, 1);
    ASSERT_EQ(seen3, 1);
}

TEST(clear_empties_table) {
    HashTable *t = fresh();

    int va = 1, vb = 2;
    hashtable_insert(t, SK("a"), &va);
    hashtable_insert(t, SK("b"), &vb);

    hashtable_clear(t);

    void *out = NULL;
    ASSERT(hashtable_get(t, SK("a"), &out) == false);
    ASSERT(hashtable_get(t, SK("b"), &out) == false);

    HashTableIter it = hashtable_iter(t);
    const void *k = NULL;
    size_t klen = 0;
    void *v = NULL;
    ASSERT(hashtable_iter_next(&it, &k, &klen, &v) == false);

    int vc = 3;
    ASSERT_EQ(hashtable_insert(t, SK("c"), &vc), HASHTABLE_OK);
    ASSERT(hashtable_get(t, SK("c"), &out) == true);
    ASSERT(out == &vc);
}

// --- host_malloc-backed mode (NULL arena) ---
//
// All existing semantics must hold when the table is allocated via
// host_malloc instead of an arena. The caller releases via
// hashtable_destroy. Probes a handful of representative operations rather
// than mirroring every test above — semantics should be identical.

TEST(host_malloc_create_and_destroy_empty) {
    HashTable *t = hashtable_create(NULL);
    ASSERT(t != NULL);
    void *out;
    ASSERT(hashtable_get(t, SK("missing"), &out) == false);
    hashtable_destroy(t);
}

TEST(host_malloc_insert_then_get) {
    HashTable *t = hashtable_create(NULL);
    int v = 42;
    ASSERT_EQ(hashtable_insert(t, SK("k"), &v), HASHTABLE_OK);
    void *out;
    ASSERT(hashtable_get(t, SK("k"), &out) == true);
    ASSERT(out == &v);
    hashtable_destroy(t);
}

// Key bytes must be copied in host_malloc mode too — mutating the caller's
// buffer after insert must not affect the stored key.
TEST(host_malloc_key_is_copied) {
    HashTable *t = hashtable_create(NULL);
    int v = 1;
    char buf[8];
    memcpy(buf, "key", 3);
    ASSERT_EQ(hashtable_insert(t, buf, 3, &v), HASHTABLE_OK);
    memset(buf, 'X', sizeof buf);
    void *out;
    ASSERT(hashtable_get(t, "key", 3, &out) == true);
    ASSERT(out == &v);
    hashtable_destroy(t);
}

TEST(host_malloc_remove_and_reinsert) {
    HashTable *t = hashtable_create(NULL);
    int v1 = 1, v2 = 2;
    hashtable_insert(t, SK("k"), &v1);
    ASSERT_EQ(hashtable_remove(t, SK("k")), HASHTABLE_REMOVE_OK);
    void *out;
    ASSERT(hashtable_get(t, SK("k"), &out) == false);
    ASSERT_EQ(hashtable_insert(t, SK("k"), &v2), HASHTABLE_OK);
    ASSERT(hashtable_get(t, SK("k"), &out) == true);
    ASSERT(out == &v2);
    hashtable_destroy(t);
}

TEST(host_malloc_iter_visits_all) {
    HashTable *t = hashtable_create(NULL);
    int a = 1, b = 2, c = 3;
    hashtable_insert(t, SK("a"), &a);
    hashtable_insert(t, SK("b"), &b);
    hashtable_insert(t, SK("c"), &c);

    HashTableIter it = hashtable_iter(t);
    const void *k = NULL;
    size_t klen = 0;
    void *v = NULL;
    int count = 0;
    while (hashtable_iter_next(&it, &k, &klen, &v))
        count++;
    ASSERT_EQ(count, 3);
    hashtable_destroy(t);
}

// `hashtable_destroy` on an arena-backed table is a no-op — must not crash
// and must not double-free the arena's contents.
TEST(arena_backed_destroy_is_noop) {
    Arena *a = arena_create();
    HashTable *t = hashtable_create(a);
    int v = 1;
    hashtable_insert(t, SK("k"), &v);
    hashtable_destroy(t); // safe no-op
    // Table still usable until arena_destroy releases it.
    void *out;
    ASSERT(hashtable_get(t, SK("k"), &out) == true);
    arena_destroy(a);
}

int main(void) {
    RUN(create_empty);
    RUN(insert_then_get);
    RUN(insert_duplicate_rejected);
    RUN(get_missing_returns_false);
    RUN(stored_null_is_distinguishable);
    RUN(update_existing);
    RUN(update_missing_rejected);
    RUN(upsert_inserts_when_absent);
    RUN(upsert_updates_when_present);
    RUN(remove_existing);
    RUN(remove_missing_rejected);
    RUN(remove_then_reinsert);
    RUN(key_is_copied_not_borrowed);
    RUN(embedded_nul_keys_distinct);
    RUN(length_distinguishes_keys);
    RUN(collisions_resolve);
    RUN(grow_preserves_entries);
    RUN(iter_empty_yields_nothing);
    RUN(iter_visits_all_once);
    RUN(clear_empties_table);

    RUN(host_malloc_create_and_destroy_empty);
    RUN(host_malloc_insert_then_get);
    RUN(host_malloc_key_is_copied);
    RUN(host_malloc_remove_and_reinsert);
    RUN(host_malloc_iter_visits_all);
    RUN(arena_backed_destroy_is_noop);

    TEST_SUMMARY();
}
