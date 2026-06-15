#ifndef _CRDT_HASHTABLE_H
#define _CRDT_HASHTABLE_H

// Allocation backing — two modes:
//   1. `hashtable_create(arena)` — node structs + key-byte copies allocated
//      in the supplied Arena. Bulk lifetime: arena_destroy frees everything.
//      No-op `hashtable_destroy` for this mode.
//   2. `hashtable_create(NULL)` — node structs + key-byte copies allocated
//      via `host_malloc`. Caller MUST call `hashtable_destroy(table)` when
//      done to release everything.
//
// Ownership:
//   Keys  — table copies key_len bytes when a new entry is inserted (insert,
//           and the insert path of upsert). Keys are raw bytes: embedded
//           NULs and the length are significant — they are not NUL-terminated
//           strings. Caller's `key` pointer may be transient (stack, freed
//           after the call). Keys returned by `hashtable_iter_next` are
//           table-owned; valid as long as the table lives. Caller must not
//           free them.
//   Values — stored as opaque `void *`; table does NOT copy. Caller owns the
//            pointed-to memory. Lifetime must outlive any get/iter that
//            reads the slot.
//
// Lifetime: For arena-backed tables, must not outlive the arena. For
// host_malloc-backed tables, must outlive any pointer returned by get/iter.
//
// Iteration: do NOT insert into or remove from the table while iterating it.

#include "arena.h"
#include <stdbool.h>
#include <stddef.h>

typedef struct HashTableNode HashTableNode;

typedef struct HashTable {
    Arena *arena; // NULL when host_malloc-backed
    HashTableNode *head;
} HashTable;

// `arena` may be NULL — in that case, the table allocates via host_malloc
// and the caller must release with hashtable_destroy.
HashTable *hashtable_create(Arena *arena);

// Release a host_malloc-backed table (frees nodes, key copies, the table
// struct itself). No-op for arena-backed tables (their arena owns the
// memory). Safe to call regardless of backing.
void hashtable_destroy(HashTable *table);

typedef enum {
    HASHTABLE_OK,
    HASHTABLE_ERR_OOM,
    HASHTABLE_ERR_KEY_EXISTS,
} HashTableInsertResult;
HashTableInsertResult hashtable_insert(HashTable *table, const void *key,
                                       size_t key_len, void *value);

// Human-readable name of a HashTableInsertResult, for logging and abort
// messages. Returns a static string ("OK", "OOM", "KEY_EXISTS", or "unknown").
const char *hashtable_insert_result_name(HashTableInsertResult r);

// Returns true if key found; sets *out to stored value (which may itself be
// NULL). Returns false if key not present; *out untouched.
bool hashtable_get(HashTable *table, const void *key, size_t key_len,
                   void **out);

typedef enum {
    HASHTABLE_REMOVE_OK,
    HASHTABLE_REMOVE_ERR_NOT_FOUND,
} HashTableRemoveResult;
HashTableRemoveResult hashtable_remove(HashTable *table, const void *key,
                                       size_t key_len);

typedef enum {
    HASHTABLE_UPDATE_OK,
    HASHTABLE_UPDATE_ERR_NOT_FOUND,
} HashTableUpdateResult;
HashTableUpdateResult hashtable_update(HashTable *table, const void *key,
                                       size_t key_len, void *value);

typedef enum {
    HASHTABLE_UPSERT_INSERTED,
    HASHTABLE_UPSERT_UPDATED,
    HASHTABLE_UPSERT_ERR_OOM,
} HashTableUpsertResult;
HashTableUpsertResult hashtable_upsert(HashTable *table, const void *key,
                                       size_t key_len, void *value);

void hashtable_clear(HashTable *table);

typedef struct {
    HashTableNode *next;
} HashTableIter;

HashTableIter hashtable_iter(HashTable *table);
bool hashtable_iter_next(HashTableIter *it, const void **key, size_t *key_len,
                         void **value);

#endif // _CRDT_HASHTABLE_H
