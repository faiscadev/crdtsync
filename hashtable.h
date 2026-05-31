#ifndef _CRDT_HASHTABLE_H
#define _CRDT_HASHTABLE_H

// Ownership:
//   Keys  — table copies key_len bytes into its arena when a new entry is
//           inserted (insert, and the insert path of upsert). Keys are raw
//           bytes: embedded NULs and the length are significant — they are not
//           NUL-terminated strings. Caller's `key` pointer may be transient
//           (stack, freed after the call). Keys returned by
//           `hashtable_iter_next` are table-owned; valid as long as the arena
//           lives. Caller must not free them.
//   Values — stored as opaque `void *`; table does NOT copy. Caller owns the
//            pointed-to memory (typically arena-allocated). Lifetime must
//            outlive any get/iter that reads the slot.
//
// Lifetime: HashTable must not outlive its arena. Resetting the arena
// invalidates every key, value, and the table itself.
//
// Iteration: do NOT insert into or remove from the table while iterating it.
// Mutation can leave the iterator's cursor pointing at an unlinked entry or
// cause entries to be skipped. Finish iterating first, then mutate.

#include "arena.h"
#include <stdbool.h>
#include <stddef.h>

typedef struct HashTableNode HashTableNode;

typedef struct HashTable {
    Arena *arena;
    HashTableNode *head;
} HashTable;

HashTable *hashtable_create(Arena *arena);

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
