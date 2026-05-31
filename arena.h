#ifndef _CRDT_ARENA_H
#define _CRDT_ARENA_H

#include <stddef.h>
#include <stdint.h>

typedef struct Arena {
    size_t size;
    size_t offset;
    uint8_t *data;
} Arena;

Arena *arena_create(uint8_t *data, size_t size);
void *arena_alloc(Arena *arena, size_t size);
size_t arena_mark(Arena *arena);
void arena_restore(Arena *arena, size_t mark);
void arena_reset(Arena *arena);

#endif // _CRDT_ARENA_H
