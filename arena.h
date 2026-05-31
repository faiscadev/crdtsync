#ifndef _CRDT_ARENA_H
#define _CRDT_ARENA_H

#include <stddef.h>
#include <stdint.h>

typedef struct Arena {
    size_t size;
    size_t offset;
    uint8_t *data;
} Arena;

// Minimum buffer size for arena_create. Worst case: the caller's buffer
// happens to be 1 byte off the required alignment, costing up to
// (_Alignof(max_align_t) - 1) bytes to align the Arena struct AND another
// (_Alignof(max_align_t) - 1) bytes to align the payload past the struct.
// Anything smaller triggers host_abort.
#define ARENA_MIN_SIZE (2 * (_Alignof(max_align_t) - 1) + sizeof(Arena))

Arena *arena_create(uint8_t *data, size_t size);
void *arena_alloc(Arena *arena, size_t size);
size_t arena_mark(Arena *arena);
void arena_restore(Arena *arena, size_t mark);
void arena_reset(Arena *arena);

#endif // _CRDT_ARENA_H
