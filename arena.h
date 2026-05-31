#ifndef _CRDT_ARENA_H
#define _CRDT_ARENA_H

#include <stddef.h>
#include <stdint.h>

// Opaque handles. Bodies live in arena.c.
typedef struct Arena Arena;
typedef struct ArenaPage ArenaPage;

// Position into an arena's allocation timeline. Returned by arena_mark and
// passed back to arena_restore. Pass-by-value; the page pointer it carries
// is owned by the arena and must not be freed by the caller.
typedef struct ArenaMark {
    ArenaPage *page;
    size_t offset;
} ArenaMark;

// Default size used for the first page and for subsequently grown pages when
// the requested allocation fits. Allocations larger than this get a dedicated
// page sized for the request.
#define ARENA_DEFAULT_PAGE 4096

Arena *arena_create(void);
void arena_destroy(Arena *arena);

void *arena_alloc(Arena *arena, size_t size);

ArenaMark arena_mark(const Arena *arena);
void arena_restore(Arena *arena, ArenaMark mark);
void arena_reset(Arena *arena);

// Stats.
size_t arena_used(const Arena *arena);     // bytes allocated right now
size_t arena_capacity(const Arena *arena); // bytes available across all pages
size_t arena_peak(const Arena *arena);     // peak `used` watermark since create

#endif // _CRDT_ARENA_H
