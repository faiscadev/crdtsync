#include "hashtabl.h"
#include "string.h"
#include <stdio.h>

typedef struct HashTablNode {
    const char *key;
    void *value;
    HashTablNode *next;
} HashTablNode;

HashTabl *hashtabl_create(Arena *arena) {
    HashTabl *table = arena_alloc(arena, sizeof(HashTabl));
    if (!table)
        return NULL;

    table->arena = arena;

    return table;
}

HashTablInsertResult _hashtabl_insert(HashTabl *table, const char *key,
                                      void *value) {
    HashTablNode *node = arena_alloc(table->arena, sizeof(HashTablNode));
    if (!node) {
        return HASHTABL_ERR_OOM;
    }

    node->key = arena_alloc(table->arena, strlen(key) + 1);
    if (!node->key) {
        return HASHTABL_ERR_OOM;
    }

    strcpy((char *)node->key, key);
    node->value = value;

    // cons into head of list
    node->next = table->head;
    table->head = node;

    return HASHTABL_OK;
}

HashTablInsertResult hashtabl_insert(HashTabl *table, const char *key,
                                     void *value) {
    if (hashtabl_get(table, key, NULL)) {
        return HASHTABL_ERR_KEY_EXISTS;
    }

    return _hashtabl_insert(table, key, value);
}

bool hashtabl_get(HashTabl *table, const char *key, void **out) {
    HashTablIter it = hashtabl_iter(table);
    const char *k;
    void *v;
    while (hashtabl_iter_next(&it, &k, &v)) {
        if (strcmp(k, key) == 0) {
            if (out) {
                *out = v;
            }
            return true;
        }
    }
    return false;
}

HashTablRemoveResult hashtabl_remove(HashTabl *table, const char *key) {
    HashTablNode *prev = NULL;
    for (HashTablNode *n = table->head; n; prev = n, n = n->next) {
        if (strcmp(n->key, key) == 0) {
            if (prev) {
                prev->next = n->next;
            } else {
                table->head = n->next;
            }
            return HASHTABL_REMOVE_OK;
        }
    }

    return HASHTABL_REMOVE_ERR_NOT_FOUND;
}

HashTablUpdateResult hashtabl_update(HashTabl *table, const char *key,
                                     void *value) {
    for (HashTablNode *n = table->head; n; n = n->next) {
        if (strcmp(n->key, key) == 0) {
            n->value = value;
            return HASHTABL_UPDATE_OK;
        }
    }

    return HASHTABL_UPDATE_ERR_NOT_FOUND;
}

HashTablUpsertResult hashtabl_upsert(HashTabl *table, const char *key,
                                     void *value) {
    if (hashtabl_update(table, key, value) == HASHTABL_UPDATE_OK) {
        return HASHTABL_UPSERT_UPDATED;
    }

    HashTablInsertResult insert_result = _hashtabl_insert(table, key, value);
    if (insert_result == HASHTABL_OK) {
        return HASHTABL_UPSERT_INSERTED;
    } else {
        return HASHTABL_UPSERT_ERR_OOM;
    }
}

void hashtabl_clear(HashTabl *table) { table->head = NULL; }

HashTablIter hashtabl_iter(HashTabl *table) {
    HashTablIter it = {0};
    it.next = table->head;
    return it;
}
bool hashtabl_iter_next(HashTablIter *it, const char **key, void **value) {
    if (it->next == NULL) {
        return false;
    }

    if (key) {
        *key = it->next->key;
    }
    if (value) {
        *value = it->next->value;
    }
    it->next = it->next->next;
    return true;
}
