#include "arena.h"
#include "string.h"

Arena *arena_create(uint8_t *data, size_t size) {
    Arena *arena = (Arena *)data;
    arena->size = size - sizeof(Arena);
    arena->offset = 0;
    arena->data = data + sizeof(Arena);
    return arena;
}

void *arena_alloc(Arena *arena, size_t size) {
    if (arena->offset + size > arena->size) {
        return NULL;
    }
    void *ptr = arena->data + arena->offset;
    arena->offset += size;
    memset(ptr, 0, size);
    return ptr;
}
