#include "arena.h"
#include "host.h"
#include "string.h"

#include <stdint.h>

struct ArenaPage {
    struct ArenaPage *next;
    size_t size;   // payload capacity
    size_t offset; // bytes used in this page
    uint8_t *data; // aligned payload start
};

struct Arena {
    ArenaPage *first;
    ArenaPage *current;
    size_t total_used;
    size_t total_capacity;
    size_t peak;
};

static size_t align_up(size_t value, size_t align) {
    return (value + align - 1) & ~(align - 1);
}

static uintptr_t align_up_ptr(uintptr_t value, size_t align) {
    return (value + align - 1) & ~(uintptr_t)(align - 1);
}

// Allocate one chained page. The single allocation holds the ArenaPage struct
// followed by alignment padding and the payload. Returns NULL on host_malloc
// failure.
static ArenaPage *new_page(size_t payload_size) {
    size_t align = _Alignof(max_align_t);
    // Reserve room for the struct + worst-case alignment slack + the payload.
    size_t alloc_size = sizeof(ArenaPage) + (align - 1) + payload_size;
    uint8_t *raw = host_malloc(alloc_size);
    if (!raw) {
        return NULL;
    }
    ArenaPage *page = (ArenaPage *)raw;
    page->next = NULL;
    uintptr_t data_at =
        align_up_ptr((uintptr_t)(raw + sizeof(ArenaPage)), align);
    page->data = (uint8_t *)data_at;
    page->size = (size_t)((uintptr_t)(raw + alloc_size) - data_at);
    page->offset = 0;
    return page;
}

Arena *arena_create(void) {
    Arena *arena = host_malloc(sizeof(Arena));
    if (!arena) {
        return NULL;
    }
    ArenaPage *page = new_page(ARENA_DEFAULT_PAGE);
    if (!page) {
        host_free(arena);
        return NULL;
    }
    arena->first = page;
    arena->current = page;
    arena->total_used = 0;
    arena->total_capacity = page->size;
    arena->peak = 0;
    return arena;
}

void arena_destroy(Arena *arena) {
    if (!arena) {
        return;
    }
    ArenaPage *page = arena->first;
    while (page) {
        ArenaPage *next = page->next;
        host_free(page);
        page = next;
    }
    host_free(arena);
}

void *arena_alloc(Arena *arena, size_t size) {
    size_t align = _Alignof(max_align_t);
    size_t aligned_size = align_up(size, align);

    ArenaPage *page = arena->current;
    if (page->offset + aligned_size > page->size) {
        // Grow: allocate a new page sized to fit the request (at least the
        // default page size).
        size_t new_payload = aligned_size > ARENA_DEFAULT_PAGE
                                 ? aligned_size
                                 : ARENA_DEFAULT_PAGE;
        ArenaPage *grown = new_page(new_payload);
        if (!grown) {
            return NULL;
        }
        page->next = grown;
        arena->current = grown;
        arena->total_capacity += grown->size;
        page = grown;
    }

    void *ptr = page->data + page->offset;
    page->offset += aligned_size;
    arena->total_used += aligned_size;
    if (arena->total_used > arena->peak) {
        arena->peak = arena->total_used;
    }
    memset(ptr, 0, size);
    return ptr;
}

ArenaMark arena_mark(const Arena *arena) {
    ArenaMark mark = {arena->current, arena->current->offset};
    return mark;
}

void arena_restore(Arena *arena, ArenaMark mark) {
    // Walk the chain to validate `mark.page` and compute the cumulative byte
    // count up to that page.
    ArenaPage *page = arena->first;
    size_t cumulative = 0;
    while (page && page != mark.page) {
        cumulative += page->offset;
        page = page->next;
    }
    if (!page) {
        host_abort("arena_restore: mark page is not in this arena");
    }
    if (mark.offset > page->offset) {
        host_abortf(
            "arena_restore: mark offset %zu exceeds page offset %zu (would "
            "advance, not rewind)",
            mark.offset, page->offset);
    }

    page->offset = mark.offset;
    ArenaPage *tail = page->next;
    page->next = NULL;
    arena->current = page;
    while (tail) {
        ArenaPage *next = tail->next;
        arena->total_capacity -= tail->size;
        host_free(tail);
        tail = next;
    }

    arena->total_used = cumulative + mark.offset;
    // Peak deliberately untouched — it's a watermark.
}

void arena_reset(Arena *arena) {
    ArenaMark zero = {arena->first, 0};
    arena_restore(arena, zero);
}

size_t arena_used(const Arena *arena) { return arena->total_used; }
size_t arena_capacity(const Arena *arena) { return arena->total_capacity; }
size_t arena_peak(const Arena *arena) { return arena->peak; }
