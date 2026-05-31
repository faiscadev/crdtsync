#include "arena.h"
#include "host.h"
#include "string.h"

#include <stdint.h>

static size_t align_up(size_t value, size_t align) {
    return (value + align - 1) & ~(align - 1);
}

static uintptr_t align_up_ptr(uintptr_t value, size_t align) {
    return (value + align - 1) & ~(uintptr_t)(align - 1);
}

Arena *arena_create(uint8_t *data, size_t size) {
    size_t align = _Alignof(max_align_t);

    // Place the Arena struct at the next aligned address inside `data`. The
    // caller's pointer may have any alignment; we round it up so the struct's
    // fields are aligned (avoids strict-alignment UB on ARM / RISC-V).
    uintptr_t struct_at = align_up_ptr((uintptr_t)data, align);
    Arena *arena = (Arena *)struct_at;
    arena->offset = 0;

    // Payload starts at the next aligned address past the struct. With this,
    // every arena_alloc returns an aligned pointer regardless of where `data`
    // landed.
    uintptr_t payload_at = align_up_ptr(struct_at + sizeof(Arena), align);
    arena->data = (uint8_t *)payload_at;

    // Whatever's left of the caller's buffer is available for allocations.
    size_t used = (size_t)(arena->data - data);
    arena->size = size - used;
    return arena;
}

void *arena_alloc(Arena *arena, size_t size) {
    size_t align = _Alignof(max_align_t);
    size_t aligned_size = align_up(size, align);

    // Bounds-check against the aligned advance so we never bump `offset` past
    // `size` — otherwise arena_mark could return a value arena_restore can't
    // reach.
    if (arena->offset + aligned_size > arena->size) {
        return NULL;
    }

    void *ptr = arena->data + arena->offset;
    arena->offset += aligned_size;
    memset(ptr, 0, size);
    return ptr;
}

size_t arena_mark(Arena *arena) { return arena->offset; }

void arena_restore(Arena *arena, size_t mark) {
    // Invariant: marks come from arena_mark, which always returns a value in
    // [0, offset]. Restoring to a mark greater than the current offset would
    // advance, not rewind — programmer error, abort loudly.
    if (mark > arena->offset) {
        host_abort("arena_restore: mark > current offset");
    }
    arena->offset = mark;
}

void arena_reset(Arena *arena) { arena->offset = 0; }
