#include <stddef.h>

static unsigned char heap[1024 * 1024];
static size_t heap_offset = 0;
void *malloc(size_t size) {
    if (heap_offset + size > sizeof(heap)) {
        return NULL;
    }
    void *ptr = heap + heap_offset;
    heap_offset += size;
    return ptr;
}

void *realloc(void *ptr, size_t size) {
    if (heap_offset + size > sizeof(heap)) {
        return NULL;
    }
    void *new_ptr = heap + heap_offset;
    heap_offset += size;
    return new_ptr;
}

int strcmp(const char *s1, const char *s2) {
    while (*s1 && (*s1 == *s2)) {
        s1++;
        s2++;
    }
    return *(unsigned char *)s1 - *(unsigned char *)s2;
}

typedef struct {
    long value;
} Counter;

typedef struct Counters {
    size_t size;
    size_t capacity;

    Counter *counters;
    const char **names;
} Counters;

Counters *counters_new() {
    Counters *counters = malloc(sizeof(Counters));
    counters->size = 0;
    counters->capacity = 0;
    counters->counters = NULL;
    counters->names = NULL;
    return counters;
}

Counter *counters_push(Counters *counters, const char *name,
                       long initial_value) {
    if (counters->size == counters->capacity) {
        size_t new_capacity =
            counters->capacity == 0 ? 4 : counters->capacity * 2;
        Counter *new_counters =
            realloc(counters->counters, new_capacity * sizeof(Counter));
        const char **new_names =
            realloc(counters->names, new_capacity * sizeof(char *));

        if (!new_counters || !new_names) {
            return NULL;
        }

        counters->counters = new_counters;
        counters->names = new_names;
        counters->capacity = new_capacity;
    }

    counters->size++;
    counters->counters[counters->size - 1].value = initial_value;
    counters->names[counters->size - 1] = name;

    return &counters->counters[counters->size - 1];
}

typedef struct {
    Counters *counters;
} Doc;

Doc *doc_new() {
    Doc *doc = malloc(sizeof(Doc));
    doc->counters = counters_new();
    return doc;
}

Counter *doc_counter(Doc *doc, const char *name, long initial_value) {
    for (size_t i = 0; i < doc->counters->size; i++) {
        if (strcmp(doc->counters->names[i], name) == 0) {
            return &doc->counters->counters[i];
        }
    }

    return counters_push(doc->counters, name, initial_value);
}

void counter_inc(Counter *counter) { counter->value++; }

void counter_dec(Counter *counter) { counter->value--; }

long counter_read(Counter *counter) { return counter->value; }
