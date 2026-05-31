#include "arena.h"
#include "test_util.h"

#include <stddef.h>
#include <stdint.h>

#define ARENA_BYTES (4 * 1024)

// All returned pointers must satisfy this alignment.
#define ARENA_ALIGN _Alignof(max_align_t)

static Arena *fresh(uint8_t *buf, size_t len) { return arena_create(buf, len); }

// --- existing behavior (regression guards) ---

TEST(create_returns_non_null) {
    uint8_t buf[ARENA_BYTES];
    ASSERT(fresh(buf, sizeof(buf)) != NULL);
}

TEST(alloc_returns_non_null) {
    uint8_t buf[ARENA_BYTES];
    Arena *a = fresh(buf, sizeof(buf));
    ASSERT(arena_alloc(a, 32) != NULL);
}

TEST(alloc_returns_distinct_pointers) {
    uint8_t buf[ARENA_BYTES];
    Arena *a = fresh(buf, sizeof(buf));
    void *p1 = arena_alloc(a, 32);
    void *p2 = arena_alloc(a, 32);
    ASSERT(p1 != p2);
}

TEST(alloc_oom_returns_null) {
    uint8_t buf[256];
    Arena *a = fresh(buf, sizeof(buf));
    ASSERT(arena_alloc(a, 4096) == NULL);
}

TEST(alloc_zeroes_memory) {
    uint8_t buf[ARENA_BYTES];
    // Pre-fill the entire backing buffer with non-zero so we'd notice if
    // arena_alloc didn't zero.
    for (size_t i = 0; i < sizeof(buf); i++) {
        buf[i] = 0xAB;
    }
    Arena *a = fresh(buf, sizeof(buf));
    uint8_t *p = arena_alloc(a, 64);
    for (int i = 0; i < 64; i++) {
        ASSERT_EQ(p[i], 0);
    }
}

// --- alignment (new contract) ---

// Every returned pointer is aligned to max_align_t (16 on most 64-bit
// targets). Without this, accessing a uint64_t or pointer in the slot is
// undefined behavior on stricter targets (ARM, RISC-V).
TEST(alloc_pointers_are_aligned) {
    uint8_t buf[ARENA_BYTES];
    Arena *a = fresh(buf, sizeof(buf));
    for (int i = 0; i < 8; i++) {
        void *p = arena_alloc(a, 32);
        ASSERT_EQ((uintptr_t)p % ARENA_ALIGN, 0);
    }
}

// Force the backing buffer to be 16-aligned. With a naive implementation that
// stores Arena bookkeeping at buf[0..23] and starts allocations at buf+24, the
// first arena_alloc returns buf+24 — which is 8-aligned, NOT 16-aligned. The
// old test passed by accident because stack uint8_t arrays often landed at
// 8-aligned-but-not-16-aligned addresses (so buf+24 became 16-aligned).
TEST(alloc_aligned_when_buffer_is_already_aligned) {
    _Alignas(16) uint8_t buf[ARENA_BYTES];
    Arena *a = fresh(buf, sizeof(buf));
    void *p = arena_alloc(a, 32);
    ASSERT_EQ((uintptr_t)p % ARENA_ALIGN, 0);
}

// Sweep every possible 16-byte offset of the arena's base. Even with all 16
// starting points, every returned pointer must be 16-aligned. At least 15 of
// these 16 offsets are guaranteed to break any "trust the buffer alignment"
// implementation.
TEST(alloc_aligned_at_every_buffer_offset) {
    _Alignas(16) uint8_t backing[ARENA_BYTES + 16];
    for (size_t off = 0; off < 16; off++) {
        Arena *a = arena_create(backing + off, ARENA_BYTES);
        for (int i = 0; i < 4; i++) {
            void *p = arena_alloc(a, 32);
            ASSERT(p != NULL);
            ASSERT_EQ((uintptr_t)p % ARENA_ALIGN, 0);
        }
    }
}

// Odd-sized allocations must not corrupt alignment of subsequent ones.
TEST(alloc_aligned_after_odd_sizes) {
    uint8_t buf[ARENA_BYTES];
    Arena *a = fresh(buf, sizeof(buf));
    size_t odd_sizes[] = {1, 3, 5, 7, 9, 11, 13, 15, 17, 33};
    for (size_t i = 0; i < sizeof(odd_sizes) / sizeof(odd_sizes[0]); i++) {
        void *p = arena_alloc(a, odd_sizes[i]);
        ASSERT(p != NULL);
        ASSERT_EQ((uintptr_t)p % ARENA_ALIGN, 0);
    }
}

// A 1-byte alloc consumes more than 1 byte of arena space because the next
// allocation has to land on the alignment boundary.
TEST(odd_size_alloc_advances_to_next_boundary) {
    uint8_t buf[ARENA_BYTES];
    Arena *a = fresh(buf, sizeof(buf));
    uint8_t *p1 = arena_alloc(a, 1);
    uint8_t *p2 = arena_alloc(a, 1);
    ASSERT(p1 != NULL && p2 != NULL);
    // Gap between p1 and p2 must be at least ARENA_ALIGN — confirms padding.
    ASSERT((size_t)(p2 - p1) >= ARENA_ALIGN);
}

// --- reset (new) ---

TEST(reset_allows_full_reuse) {
    uint8_t buf[ARENA_BYTES];
    Arena *a = fresh(buf, sizeof(buf));

    void *p1 = arena_alloc(a, 128);
    ASSERT(p1 != NULL);

    arena_reset(a);

    void *p2 = arena_alloc(a, 128);
    ASSERT(p2 != NULL);
    ASSERT(p2 == p1); // same address: reset rewound to the start
}

TEST(reset_zeroes_memory_on_next_alloc) {
    uint8_t buf[ARENA_BYTES];
    Arena *a = fresh(buf, sizeof(buf));

    uint8_t *p = arena_alloc(a, 64);
    for (int i = 0; i < 64; i++) {
        p[i] = 0xFF; // dirty the memory
    }
    arena_reset(a);

    uint8_t *p2 = arena_alloc(a, 64);
    for (int i = 0; i < 64; i++) {
        ASSERT_EQ(p2[i], 0); // arena_alloc still zero-fills on reuse
    }
}

TEST(reset_recovers_oom_capacity) {
    uint8_t buf[1024];
    Arena *a = fresh(buf, sizeof(buf));

    // Drain.
    while (arena_alloc(a, 64) != NULL) {
    }
    ASSERT(arena_alloc(a, 64) == NULL);

    arena_reset(a);
    ASSERT(arena_alloc(a, 64) != NULL);
}

// --- mark / restore (new) ---

TEST(restore_to_mark_undoes_intervening_allocs) {
    uint8_t buf[ARENA_BYTES];
    Arena *a = fresh(buf, sizeof(buf));

    void *p1 = arena_alloc(a, 128);
    ASSERT(p1 != NULL);

    size_t mark = arena_mark(a);

    void *p2 = arena_alloc(a, 128);
    ASSERT(p2 != NULL);

    arena_restore(a, mark);

    void *p3 = arena_alloc(a, 128);
    ASSERT(p3 == p2); // same address as the rolled-back alloc
}

// Memory allocated BEFORE the mark must remain valid after restore — restore
// only rolls back later allocations, not earlier ones.
TEST(memory_before_mark_remains_valid) {
    uint8_t buf[ARENA_BYTES];
    Arena *a = fresh(buf, sizeof(buf));

    uint8_t *p1 = arena_alloc(a, 64);
    for (int i = 0; i < 64; i++) {
        p1[i] = (uint8_t)(0x80 | i);
    }

    size_t mark = arena_mark(a);
    arena_alloc(a, 256); // scratch
    arena_restore(a, mark);

    for (int i = 0; i < 64; i++) {
        ASSERT_EQ(p1[i], (uint8_t)(0x80 | i));
    }
}

// arena_restore must accept marks taken before any allocs (offset 0).
TEST(restore_to_zero_is_equivalent_to_reset) {
    uint8_t buf[ARENA_BYTES];
    Arena *a = fresh(buf, sizeof(buf));

    size_t initial = arena_mark(a);
    void *p1 = arena_alloc(a, 128);
    ASSERT(p1 != NULL);

    arena_alloc(a, 64);
    arena_alloc(a, 64);

    arena_restore(a, initial);

    void *p2 = arena_alloc(a, 128);
    ASSERT(p2 == p1);
}

// Nested marks: restore the inner first, then alloc; then restore the outer,
// then alloc; addresses come back in the expected order.
TEST(nested_marks_restore_in_lifo_order) {
    uint8_t buf[ARENA_BYTES];
    Arena *a = fresh(buf, sizeof(buf));

    void *p_outer = arena_alloc(a, 128);
    ASSERT(p_outer != NULL);

    size_t outer_mark = arena_mark(a);
    void *p_inner = arena_alloc(a, 128);
    ASSERT(p_inner != NULL);

    size_t inner_mark = arena_mark(a);
    arena_alloc(a, 64);
    arena_alloc(a, 64);

    arena_restore(a, inner_mark);
    void *p_after_inner = arena_alloc(a, 64);
    ASSERT(p_after_inner != NULL);
    ASSERT(p_after_inner > p_inner); // beyond the inner_mark's prior allocs

    arena_restore(a, outer_mark);
    void *p_after_outer = arena_alloc(a, 128);
    ASSERT(p_after_outer == p_inner); // rewound past inner's allocs
}

// After restore, the next alloc must still return zeroed memory — scratch
// data left behind by the rolled-back allocs must not leak into the next use.
TEST(restore_then_alloc_returns_zeroed_memory) {
    uint8_t buf[ARENA_BYTES];
    Arena *a = fresh(buf, sizeof(buf));

    size_t mark = arena_mark(a);

    uint8_t *scratch = arena_alloc(a, 128);
    for (int i = 0; i < 128; i++) {
        scratch[i] = 0xCC;
    }

    arena_restore(a, mark);

    uint8_t *fresh_p = arena_alloc(a, 128);
    for (int i = 0; i < 128; i++) {
        ASSERT_EQ(fresh_p[i], 0);
    }
}

int main(void) {
    RUN(create_returns_non_null);
    RUN(alloc_returns_non_null);
    RUN(alloc_returns_distinct_pointers);
    RUN(alloc_oom_returns_null);
    RUN(alloc_zeroes_memory);

    RUN(alloc_pointers_are_aligned);
    RUN(alloc_aligned_when_buffer_is_already_aligned);
    RUN(alloc_aligned_at_every_buffer_offset);
    RUN(alloc_aligned_after_odd_sizes);
    RUN(odd_size_alloc_advances_to_next_boundary);

    RUN(reset_allows_full_reuse);
    RUN(reset_zeroes_memory_on_next_alloc);
    RUN(reset_recovers_oom_capacity);

    RUN(restore_to_mark_undoes_intervening_allocs);
    RUN(memory_before_mark_remains_valid);
    RUN(restore_to_zero_is_equivalent_to_reset);
    RUN(nested_marks_restore_in_lifo_order);
    RUN(restore_then_alloc_returns_zeroed_memory);

    TEST_SUMMARY();
}
