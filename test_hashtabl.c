#include <string.h>

#include "arena.h"
#include "hashtabl.h"
#include "test_util.h"
#include <assert.h>

// Backing buffer big enough for functional tests (keys + buckets + rehash
// slack).
#define ARENA_BYTES (64 * 1024)

// Helper: build a fresh table on a caller-owned stack buffer.
// Returns table; writes the arena pointer through `out_arena` (unused by most
// tests).
static HashTabl *fresh(uint8_t *buf, size_t buf_len) {
    Arena *arena = arena_create(buf, buf_len);
    HashTabl *table = hashtabl_create(arena);
    assert(table != NULL);
    return table;
}

TEST(create_empty) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    void *out = (void *)0xdead;
    ASSERT(hashtabl_get(t, "missing", &out) == false);
    // out must be untouched on miss.
    ASSERT(out == (void *)0xdead);
}

TEST(insert_then_get) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    int v = 42;
    ASSERT_EQ(hashtabl_insert(t, "answer", &v), HASHTABL_OK);

    void *out = NULL;
    ASSERT(hashtabl_get(t, "answer", &out) == true);
    ASSERT(out == &v);
    ASSERT_EQ(*(int *)out, 42);
}

TEST(insert_duplicate_rejected) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    int a = 1, b = 2;
    ASSERT_EQ(hashtabl_insert(t, "k", &a), HASHTABL_OK);
    ASSERT_EQ(hashtabl_insert(t, "k", &b), HASHTABL_ERR_KEY_EXISTS);

    // Value must remain the first one.
    void *out = NULL;
    ASSERT(hashtabl_get(t, "k", &out) == true);
    ASSERT(out == &a);
}

TEST(get_missing_returns_false) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    int v = 7;
    hashtabl_insert(t, "present", &v);
    printf("Inserted key 'present' with value %d\n", v);

    void *out = NULL;
    ASSERT(hashtabl_get(t, "absent", &out) == false);
}

// The whole point of the bool/out-param API: NULL is a storable value,
// distinguishable from "not found".
TEST(stored_null_is_distinguishable) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    ASSERT_EQ(hashtabl_insert(t, "nullval", NULL), HASHTABL_OK);

    void *out = (void *)0xbeef;
    ASSERT(hashtabl_get(t, "nullval", &out) == true);
    ASSERT(out == NULL);

    // A genuinely missing key still reports false.
    out = (void *)0xbeef;
    ASSERT(hashtabl_get(t, "other", &out) == false);
}

TEST(update_existing) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    int a = 1, b = 2;
    hashtabl_insert(t, "k", &a);
    ASSERT_EQ(hashtabl_update(t, "k", &b), HASHTABL_UPDATE_OK);

    void *out = NULL;
    ASSERT(hashtabl_get(t, "k", &out) == true);
    ASSERT(out == &b);
}

TEST(update_missing_rejected) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    int b = 2;
    ASSERT_EQ(hashtabl_update(t, "ghost", &b), HASHTABL_UPDATE_ERR_NOT_FOUND);
}

TEST(upsert_inserts_when_absent) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    int v = 5;
    ASSERT_EQ(hashtabl_upsert(t, "k", &v), HASHTABL_UPSERT_INSERTED);

    void *out = NULL;
    ASSERT(hashtabl_get(t, "k", &out) == true);
    ASSERT(out == &v);
}

TEST(upsert_updates_when_present) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    int a = 1, b = 2;
    ASSERT_EQ(hashtabl_upsert(t, "k", &a), HASHTABL_UPSERT_INSERTED);
    ASSERT_EQ(hashtabl_upsert(t, "k", &b), HASHTABL_UPSERT_UPDATED);

    void *out = NULL;
    ASSERT(hashtabl_get(t, "k", &out) == true);
    ASSERT(out == &b);
}

TEST(remove_existing) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    int v = 9;
    hashtabl_insert(t, "k", &v);
    ASSERT_EQ(hashtabl_remove(t, "k"), HASHTABL_REMOVE_OK);

    void *out = NULL;
    ASSERT(hashtabl_get(t, "k", &out) == false);
}

TEST(remove_missing_rejected) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    ASSERT_EQ(hashtabl_remove(t, "nope"), HASHTABL_REMOVE_ERR_NOT_FOUND);
}

// After remove, the slot must be reusable (no tombstone wreckage blocking
// reinsert).
TEST(remove_then_reinsert) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    int a = 1, b = 2;
    hashtabl_insert(t, "k", &a);
    hashtabl_remove(t, "k");
    ASSERT_EQ(hashtabl_insert(t, "k", &b), HASHTABL_OK);

    void *out = NULL;
    ASSERT(hashtabl_get(t, "k", &out) == true);
    ASSERT(out == &b);
}

// Validates documented key-copy semantics: the table must copy key bytes,
// so mutating the caller's buffer after insert must not corrupt the entry.
TEST(key_is_copied_not_borrowed) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    char key[8];
    strcpy(key, "stable");
    int v = 123;
    hashtabl_insert(t, key, &v);

    // Scribble over the caller's buffer.
    strcpy(key, "ZZZZZZ");

    void *out = NULL;
    ASSERT(hashtabl_get(t, "stable", &out) == true);
    ASSERT(out == &v);

    // The mutated string must NOT resolve to the stored entry.
    out = NULL;
    ASSERT(hashtabl_get(t, "ZZZZZZ", &out) == false);
}

// Distinct keys that collide into the same bucket must both be retrievable.
// (Independent of the hash fn: with initial size 2 and several keys, collisions
// are forced by pigeonhole.)
TEST(collisions_resolve) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    int va = 1, vb = 2, vc = 3, vd = 4;
    ASSERT_EQ(hashtabl_insert(t, "alpha", &va), HASHTABL_OK);
    ASSERT_EQ(hashtabl_insert(t, "bravo", &vb), HASHTABL_OK);
    ASSERT_EQ(hashtabl_insert(t, "charlie", &vc), HASHTABL_OK);
    ASSERT_EQ(hashtabl_insert(t, "delta", &vd), HASHTABL_OK);

    void *out = NULL;
    ASSERT(hashtabl_get(t, "alpha", &out) == true);
    ASSERT(out == &va);
    ASSERT(hashtabl_get(t, "bravo", &out) == true);
    ASSERT(out == &vb);
    ASSERT(hashtabl_get(t, "charlie", &out) == true);
    ASSERT(out == &vc);
    ASSERT(hashtabl_get(t, "delta", &out) == true);
    ASSERT(out == &vd);
}

// Insert many more than initial size to force at least one grow/rehash.
// All entries must survive the rehash.
TEST(grow_preserves_entries) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    enum { N = 200 };
    static int vals[N];
    static char keys[N][16];
    for (int i = 0; i < N; i++) {
        vals[i] = i * 10;
        // keys: "key0".."key199"
        int n = i, p = 0;
        char tmp[16];
        keys[i][p++] = 'k';
        keys[i][p++] = 'e';
        keys[i][p++] = 'y';
        int len = 0;
        if (n == 0)
            tmp[len++] = '0';
        while (n > 0) {
            tmp[len++] = (char)('0' + n % 10);
            n /= 10;
        }
        while (len > 0)
            keys[i][p++] = tmp[--len];
        keys[i][p] = '\0';
        ASSERT_EQ(hashtabl_insert(t, keys[i], &vals[i]), HASHTABL_OK);
    }

    for (int i = 0; i < N; i++) {
        void *out = NULL;
        ASSERT(hashtabl_get(t, keys[i], &out) == true);
        ASSERT(out == &vals[i]);
    }
}

TEST(iter_empty_yields_nothing) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    HashTablIter it = hashtabl_iter(t);
    const char *k = NULL;
    void *v = NULL;
    ASSERT(hashtabl_iter_next(&it, &k, &v) == false);
}

// Iteration must visit every entry exactly once (order unspecified).
TEST(iter_visits_all_once) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    int va = 1, vb = 2, vc = 3;
    hashtabl_insert(t, "one", &va);
    hashtabl_insert(t, "two", &vb);
    hashtabl_insert(t, "three", &vc);

    int seen_one = 0, seen_two = 0, seen_three = 0, total = 0;

    HashTablIter it = hashtabl_iter(t);
    const char *k = NULL;
    void *v = NULL;
    while (hashtabl_iter_next(&it, &k, &v)) {
        total++;
        if (strcmp(k, "one") == 0) {
            seen_one++;
            ASSERT(v == &va);
        }
        if (strcmp(k, "two") == 0) {
            seen_two++;
            ASSERT(v == &vb);
        }
        if (strcmp(k, "three") == 0) {
            seen_three++;
            ASSERT(v == &vc);
        }
    }

    ASSERT_EQ(total, 3);
    ASSERT_EQ(seen_one, 1);
    ASSERT_EQ(seen_two, 1);
    ASSERT_EQ(seen_three, 1);
}

TEST(clear_empties_table) {
    uint8_t buf[ARENA_BYTES];
    HashTabl *t = fresh(buf, sizeof(buf));

    int va = 1, vb = 2;
    hashtabl_insert(t, "a", &va);
    hashtabl_insert(t, "b", &vb);

    hashtabl_clear(t);

    void *out = NULL;
    ASSERT(hashtabl_get(t, "a", &out) == false);
    ASSERT(hashtabl_get(t, "b", &out) == false);

    // Iteration after clear yields nothing.
    HashTablIter it = hashtabl_iter(t);
    const char *k = NULL;
    void *v = NULL;
    ASSERT(hashtabl_iter_next(&it, &k, &v) == false);

    // Table is reusable after clear.
    int vc = 3;
    ASSERT_EQ(hashtabl_insert(t, "c", &vc), HASHTABL_OK);
    ASSERT(hashtabl_get(t, "c", &out) == true);
    ASSERT(out == &vc);
}

// With a deliberately tiny arena, inserts must eventually report OOM
// rather than corrupting memory or succeeding past the buffer.
TEST(oom_when_arena_exhausted) {
    // Small buffer: enough for the table header + a few entries, not unbounded.
    uint8_t buf[256];
    Arena *arena = arena_create(buf, sizeof(buf));
    HashTabl *t = hashtabl_create(arena);
    // create itself may fail if buffer is too small; if so, nothing to test.
    if (t == NULL)
        return;

    static int vals[1024];
    int got_oom = 0;
    for (int i = 0; i < 1024; i++) {
        vals[i] = i;
        char key[16];
        // "k0".."k1023"
        int n = i, p = 0;
        char tmp[16];
        key[p++] = 'k';
        int len = 0;
        if (n == 0)
            tmp[len++] = '0';
        while (n > 0) {
            tmp[len++] = (char)('0' + n % 10);
            n /= 10;
        }
        while (len > 0)
            key[p++] = tmp[--len];
        key[p] = '\0';

        if (hashtabl_insert(t, key, &vals[i]) == HASHTABL_ERR_OOM) {
            got_oom = 1;
            break;
        }
    }
    ASSERT(got_oom == 1);
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
    RUN(collisions_resolve);
    RUN(grow_preserves_entries);
    RUN(iter_empty_yields_nothing);
    RUN(iter_visits_all_once);
    RUN(clear_empties_table);
    RUN(oom_when_arena_exhausted);
    TEST_SUMMARY();
}
