#include "arena.h"
#include "test_util.h"

#include <stddef.h>
#include <stdint.h>

// All returned pointers must satisfy this alignment.
#define ARENA_ALIGN _Alignof(max_align_t)

// --- create / destroy / basic alloc ---

TEST(create_returns_non_null) {
    Arena *a = arena_create();
    ASSERT(a != NULL);
    arena_destroy(a);
}

TEST(destroy_after_allocs_does_not_crash) {
    Arena *a = arena_create();
    arena_alloc(a, 64);
    arena_alloc(a, 4096); // probably grows
    arena_destroy(a);
}

TEST(alloc_returns_non_null) {
    Arena *a = arena_create();
    ASSERT(arena_alloc(a, 32) != NULL);
    arena_destroy(a);
}

TEST(alloc_returns_distinct_pointers) {
    Arena *a = arena_create();
    void *p1 = arena_alloc(a, 32);
    void *p2 = arena_alloc(a, 32);
    ASSERT(p1 != p2);
    arena_destroy(a);
}

TEST(alloc_zeroes_memory) {
    Arena *a = arena_create();
    uint8_t *p = arena_alloc(a, 64);
    for (int i = 0; i < 64; i++) {
        ASSERT_EQ(p[i], 0);
    }
    // Dirty the memory and ensure a *fresh* alloc still returns zeros.
    for (int i = 0; i < 64; i++) {
        p[i] = 0xAB;
    }
    uint8_t *q = arena_alloc(a, 64);
    for (int i = 0; i < 64; i++) {
        ASSERT_EQ(q[i], 0);
    }
    arena_destroy(a);
}

// --- alignment of returned pointers ---

TEST(alloc_pointers_are_aligned) {
    Arena *a = arena_create();
    for (int i = 0; i < 8; i++) {
        void *p = arena_alloc(a, 32);
        ASSERT_EQ((uintptr_t)p % ARENA_ALIGN, 0);
    }
    arena_destroy(a);
}

TEST(alloc_aligned_after_odd_sizes) {
    Arena *a = arena_create();
    size_t odd_sizes[] = {1, 3, 5, 7, 9, 11, 13, 15, 17, 33};
    for (size_t i = 0; i < sizeof(odd_sizes) / sizeof(odd_sizes[0]); i++) {
        void *p = arena_alloc(a, odd_sizes[i]);
        ASSERT(p != NULL);
        ASSERT_EQ((uintptr_t)p % ARENA_ALIGN, 0);
    }
    arena_destroy(a);
}

// A 1-byte alloc consumes more than 1 byte because the next allocation must
// land on the alignment boundary.
TEST(odd_size_alloc_advances_to_next_boundary) {
    Arena *a = arena_create();
    uint8_t *p1 = arena_alloc(a, 1);
    uint8_t *p2 = arena_alloc(a, 1);
    ASSERT(p1 != NULL && p2 != NULL);
    // p1 and p2 are guaranteed to be on the same first page (no growth yet).
    ASSERT((size_t)(p2 - p1) >= ARENA_ALIGN);
    arena_destroy(a);
}

// --- reset ---

TEST(reset_allows_full_reuse) {
    Arena *a = arena_create();
    void *p1 = arena_alloc(a, 128);
    ASSERT(p1 != NULL);
    arena_reset(a);
    void *p2 = arena_alloc(a, 128);
    ASSERT(p2 != NULL);
    ASSERT(p2 == p1); // reset rewound to the start of the first page
    arena_destroy(a);
}

TEST(reset_zeroes_memory_on_next_alloc) {
    Arena *a = arena_create();
    uint8_t *p = arena_alloc(a, 64);
    for (int i = 0; i < 64; i++) {
        p[i] = 0xFF;
    }
    arena_reset(a);
    uint8_t *p2 = arena_alloc(a, 64);
    for (int i = 0; i < 64; i++) {
        ASSERT_EQ(p2[i], 0);
    }
    arena_destroy(a);
}

// --- mark / restore ---

TEST(restore_to_mark_undoes_intervening_allocs) {
    Arena *a = arena_create();
    void *p1 = arena_alloc(a, 128);
    ASSERT(p1 != NULL);

    ArenaMark mark = arena_mark(a);

    void *p2 = arena_alloc(a, 128);
    ASSERT(p2 != NULL);

    arena_restore(a, mark);

    void *p3 = arena_alloc(a, 128);
    ASSERT(p3 == p2); // same address as the rolled-back alloc
    arena_destroy(a);
}

// Memory allocated BEFORE the mark must remain valid after restore.
TEST(memory_before_mark_remains_valid) {
    Arena *a = arena_create();
    uint8_t *p1 = arena_alloc(a, 64);
    for (int i = 0; i < 64; i++) {
        p1[i] = (uint8_t)(0x80 | i);
    }
    ArenaMark mark = arena_mark(a);
    arena_alloc(a, 256);
    arena_restore(a, mark);

    for (int i = 0; i < 64; i++) {
        ASSERT_EQ(p1[i], (uint8_t)(0x80 | i));
    }
    arena_destroy(a);
}

TEST(restore_to_zero_is_equivalent_to_reset) {
    Arena *a = arena_create();
    ArenaMark initial = arena_mark(a);
    void *p1 = arena_alloc(a, 128);
    ASSERT(p1 != NULL);
    arena_alloc(a, 64);
    arena_alloc(a, 64);
    arena_restore(a, initial);

    void *p2 = arena_alloc(a, 128);
    ASSERT(p2 == p1);
    arena_destroy(a);
}

TEST(nested_marks_restore_in_lifo_order) {
    Arena *a = arena_create();
    void *p_outer = arena_alloc(a, 128);
    ASSERT(p_outer != NULL);

    ArenaMark outer_mark = arena_mark(a);
    void *p_inner = arena_alloc(a, 128);
    ASSERT(p_inner != NULL);

    ArenaMark inner_mark = arena_mark(a);
    arena_alloc(a, 64);
    arena_alloc(a, 64);

    arena_restore(a, inner_mark);
    void *p_after_inner = arena_alloc(a, 64);
    ASSERT(p_after_inner != NULL);
    ASSERT(p_after_inner > p_inner);

    arena_restore(a, outer_mark);
    void *p_after_outer = arena_alloc(a, 128);
    ASSERT(p_after_outer == p_inner);
    arena_destroy(a);
}

// After restore, the next alloc must return zeroed memory — scratch data from
// the rolled-back allocs must not leak.
TEST(restore_then_alloc_returns_zeroed_memory) {
    Arena *a = arena_create();
    ArenaMark mark = arena_mark(a);

    uint8_t *scratch = arena_alloc(a, 128);
    for (int i = 0; i < 128; i++) {
        scratch[i] = 0xCC;
    }

    arena_restore(a, mark);

    uint8_t *fresh_p = arena_alloc(a, 128);
    for (int i = 0; i < 128; i++) {
        ASSERT_EQ(fresh_p[i], 0);
    }
    arena_destroy(a);
}

// --- growth (chained pages backed by host_malloc) ---

// A request larger than the first page's capacity must succeed — the arena
// allocates a new page.
TEST(alloc_beyond_first_page_grows_capacity) {
    Arena *a = arena_create();
    size_t cap_before = arena_capacity(a);
    void *p = arena_alloc(a, cap_before * 4);
    ASSERT(p != NULL);
    ASSERT(arena_capacity(a) > cap_before);
    arena_destroy(a);
}

// A request larger than ARENA_DEFAULT_PAGE must still succeed; the arena
// should allocate a dedicated page sized for the request.
TEST(alloc_larger_than_default_page_grows_to_fit) {
    Arena *a = arena_create();
    void *p = arena_alloc(a, ARENA_DEFAULT_PAGE * 4);
    ASSERT(p != NULL);
    ASSERT_EQ((uintptr_t)p % ARENA_ALIGN, 0);
    arena_destroy(a);
}

// Earlier allocations must not move or get clobbered when later allocations
// trigger growth.
TEST(earlier_allocs_survive_growth) {
    Arena *a = arena_create();
    uint8_t *first = arena_alloc(a, 64);
    ASSERT(first != NULL);
    for (int i = 0; i < 64; i++) {
        first[i] = (uint8_t)(0x80 | i);
    }
    for (int i = 0; i < 50; i++) {
        ASSERT(arena_alloc(a, 256) != NULL);
    }
    for (int i = 0; i < 64; i++) {
        ASSERT_EQ(first[i], (uint8_t)(0x80 | i));
    }
    arena_destroy(a);
}

TEST(allocs_on_secondary_pages_are_aligned) {
    Arena *a = arena_create();
    for (int i = 0; i < 100; i++) {
        void *p = arena_alloc(a, 33);
        ASSERT(p != NULL);
        ASSERT_EQ((uintptr_t)p % ARENA_ALIGN, 0);
    }
    arena_destroy(a);
}

// arena_reset must free secondary pages, returning capacity to the first
// page's payload size.
TEST(reset_frees_secondary_pages) {
    Arena *a = arena_create();
    size_t cap_initial = arena_capacity(a);
    for (int i = 0; i < 100; i++) {
        ASSERT(arena_alloc(a, 256) != NULL);
    }
    ASSERT(arena_capacity(a) > cap_initial);

    arena_reset(a);
    ASSERT_EQ(arena_capacity(a), cap_initial);
    arena_destroy(a);
}

// arena_restore must free any pages allocated past the mark.
TEST(restore_frees_secondary_pages_past_mark) {
    Arena *a = arena_create();
    size_t cap_initial = arena_capacity(a);
    ArenaMark mark = arena_mark(a);

    for (int i = 0; i < 100; i++) {
        ASSERT(arena_alloc(a, 256) != NULL);
    }
    ASSERT(arena_capacity(a) > cap_initial);

    arena_restore(a, mark);
    ASSERT_EQ(arena_capacity(a), cap_initial);
    arena_destroy(a);
}

// --- stats: used / capacity / peak ---

TEST(used_starts_at_zero) {
    Arena *a = arena_create();
    ASSERT_EQ(arena_used(a), 0);
    arena_destroy(a);
}

TEST(used_advances_by_aligned_size) {
    Arena *a = arena_create();
    size_t before = arena_used(a);
    arena_alloc(a, 7);
    size_t after_one = arena_used(a);
    ASSERT(after_one >= before + 7);
    ASSERT(after_one - before <= 7 + ARENA_ALIGN);

    arena_alloc(a, 7);
    size_t after_two = arena_used(a);
    ASSERT(after_two - after_one >= 7);
    arena_destroy(a);
}

TEST(used_returns_to_zero_after_reset) {
    Arena *a = arena_create();
    arena_alloc(a, 200);
    arena_alloc(a, 200);
    ASSERT(arena_used(a) > 0);
    arena_reset(a);
    ASSERT_EQ(arena_used(a), 0);
    arena_destroy(a);
}

TEST(restore_rewinds_used_to_mark_time) {
    Arena *a = arena_create();
    arena_alloc(a, 100);
    size_t used_at_mark = arena_used(a);
    ArenaMark mark = arena_mark(a);
    arena_alloc(a, 100);
    arena_alloc(a, 100);

    arena_restore(a, mark);
    ASSERT_EQ(arena_used(a), used_at_mark);
    arena_destroy(a);
}

TEST(capacity_at_least_covers_used) {
    Arena *a = arena_create();
    arena_alloc(a, 200);
    ASSERT(arena_capacity(a) >= arena_used(a));
    arena_destroy(a);
}

TEST(peak_starts_at_zero) {
    Arena *a = arena_create();
    ASSERT_EQ(arena_peak(a), 0);
    arena_destroy(a);
}

TEST(peak_tracks_high_water_mark) {
    Arena *a = arena_create();
    arena_alloc(a, 300);
    arena_alloc(a, 300);
    size_t high = arena_used(a);
    ASSERT_EQ(arena_peak(a), high);

    arena_reset(a);
    ASSERT_EQ(arena_used(a), 0);
    // Peak does NOT decrease — it's a watermark.
    ASSERT_EQ(arena_peak(a), high);
    arena_destroy(a);
}

TEST(peak_only_grows) {
    Arena *a = arena_create();
    arena_alloc(a, 500);
    size_t peak_after_first = arena_peak(a);

    ArenaMark mark = arena_mark(a);
    arena_alloc(a, 500);
    size_t peak_after_two = arena_peak(a);
    ASSERT(peak_after_two >= peak_after_first);

    arena_restore(a, mark);
    ASSERT_EQ(arena_peak(a), peak_after_two);

    arena_alloc(a, 100);
    ASSERT_EQ(arena_peak(a), peak_after_two);
    arena_destroy(a);
}

int main(void) {
    RUN(create_returns_non_null);
    RUN(destroy_after_allocs_does_not_crash);
    RUN(alloc_returns_non_null);
    RUN(alloc_returns_distinct_pointers);
    RUN(alloc_zeroes_memory);

    RUN(alloc_pointers_are_aligned);
    RUN(alloc_aligned_after_odd_sizes);
    RUN(odd_size_alloc_advances_to_next_boundary);

    RUN(reset_allows_full_reuse);
    RUN(reset_zeroes_memory_on_next_alloc);

    RUN(restore_to_mark_undoes_intervening_allocs);
    RUN(memory_before_mark_remains_valid);
    RUN(restore_to_zero_is_equivalent_to_reset);
    RUN(nested_marks_restore_in_lifo_order);
    RUN(restore_then_alloc_returns_zeroed_memory);

    RUN(alloc_beyond_first_page_grows_capacity);
    RUN(alloc_larger_than_default_page_grows_to_fit);
    RUN(earlier_allocs_survive_growth);
    RUN(allocs_on_secondary_pages_are_aligned);
    RUN(reset_frees_secondary_pages);
    RUN(restore_frees_secondary_pages_past_mark);

    RUN(used_starts_at_zero);
    RUN(used_advances_by_aligned_size);
    RUN(used_returns_to_zero_after_reset);
    RUN(restore_rewinds_used_to_mark_time);
    RUN(capacity_at_least_covers_used);
    RUN(peak_starts_at_zero);
    RUN(peak_tracks_high_water_mark);
    RUN(peak_only_grows);

    TEST_SUMMARY();
}
