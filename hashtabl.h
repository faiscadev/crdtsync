#ifndef _CRDT_HASHTABL_H
#define _CRDT_HASHTABL_H

// Ownership:
//   Keys  — table copies the bytes into its arena on insert/update/upsert.
//           Caller's `key` pointer may be transient (stack, freed after the call).
//           Keys returned by `hashtabl_iter_next` are table-owned; valid as long as
//           the arena lives. Caller must not free them.
//   Values — stored as opaque `void *`; table does NOT copy. Caller owns the
//            pointed-to memory (typically arena-allocated). Lifetime must outlive
//            any get/iter that reads the slot.
//
// Lifetime: HashTabl must not outlive its arena. Resetting the arena invalidates
// every key, value, and the table itself.
//
// Iteration: do NOT insert into or remove from the table while iterating it.
// Mutation can leave the iterator's cursor pointing at an unlinked entry or
// cause entries to be skipped. Finish iterating first, then mutate.

#include <stddef.h>
#include <stdbool.h>
#include "arena.h"

typedef struct HashTablNode HashTablNode;

typedef struct HashTabl {
    Arena *arena;
    HashTablNode *head;
} HashTabl;

HashTabl *hashtabl_create(Arena *arena);

typedef enum {
    HASHTABL_OK,
    HASHTABL_ERR_OOM,
    HASHTABL_ERR_KEY_EXISTS,
} HashTablInsertResult;
HashTablInsertResult hashtabl_insert(HashTabl *table, const char *key, void *value);

// Returns true if key found; sets *out to stored value (which may itself be NULL).
// Returns false if key not present; *out untouched.
bool hashtabl_get(HashTabl *table, const char *key, void **out);

typedef enum {
    HASHTABL_REMOVE_OK,
    HASHTABL_REMOVE_ERR_NOT_FOUND,
} HashTablRemoveResult;
HashTablRemoveResult hashtabl_remove(HashTabl *table, const char *key);

typedef enum {
    HASHTABL_UPDATE_OK,
    HASHTABL_UPDATE_ERR_NOT_FOUND,
} HashTablUpdateResult;
HashTablUpdateResult hashtabl_update(HashTabl *table, const char *key, void *value);

typedef enum {
    HASHTABL_UPSERT_INSERTED,
    HASHTABL_UPSERT_UPDATED,
    HASHTABL_UPSERT_ERR_OOM,
} HashTablUpsertResult;
HashTablUpsertResult hashtabl_upsert(HashTabl *table, const char *key, void *value);

void hashtabl_clear(HashTabl *table);

typedef struct {
    HashTablNode *next;
} HashTablIter;

HashTablIter hashtabl_iter(HashTabl *table);
bool hashtabl_iter_next(HashTablIter *it, const char **key, void **value);

#endif // _CRDT_HASHTABL_H
