#include "hashtable.h"
#include "string.h"
#include <stdio.h>

typedef struct HashTableNode {
    const char *key;
    void *value;
    HashTableNode *next;
} HashTableNode;

HashTable *hashtable_create(Arena *arena) {
    HashTable *table = arena_alloc(arena, sizeof(HashTable));
    if (!table)
        return NULL;

    table->arena = arena;

    return table;
}

HashTableInsertResult _hashtable_insert(HashTable *table, const char *key,
                                      void *value) {
    HashTableNode *node = arena_alloc(table->arena, sizeof(HashTableNode));
    if (!node) {
        return HASHTABLE_ERR_OOM;
    }

    node->key = arena_alloc(table->arena, strlen(key) + 1);
    if (!node->key) {
        return HASHTABLE_ERR_OOM;
    }

    strcpy((char *)node->key, key);
    node->value = value;

    // cons into head of list
    node->next = table->head;
    table->head = node;

    return HASHTABLE_OK;
}

HashTableInsertResult hashtable_insert(HashTable *table, const char *key,
                                     void *value) {
    if (hashtable_get(table, key, NULL)) {
        return HASHTABLE_ERR_KEY_EXISTS;
    }

    return _hashtable_insert(table, key, value);
}

bool hashtable_get(HashTable *table, const char *key, void **out) {
    HashTableIter it = hashtable_iter(table);
    const char *k;
    void *v;
    while (hashtable_iter_next(&it, &k, &v)) {
        if (strcmp(k, key) == 0) {
            if (out) {
                *out = v;
            }
            return true;
        }
    }
    return false;
}

HashTableRemoveResult hashtable_remove(HashTable *table, const char *key) {
    HashTableNode *prev = NULL;
    for (HashTableNode *n = table->head; n; prev = n, n = n->next) {
        if (strcmp(n->key, key) == 0) {
            if (prev) {
                prev->next = n->next;
            } else {
                table->head = n->next;
            }
            return HASHTABLE_REMOVE_OK;
        }
    }

    return HASHTABLE_REMOVE_ERR_NOT_FOUND;
}

HashTableUpdateResult hashtable_update(HashTable *table, const char *key,
                                     void *value) {
    for (HashTableNode *n = table->head; n; n = n->next) {
        if (strcmp(n->key, key) == 0) {
            n->value = value;
            return HASHTABLE_UPDATE_OK;
        }
    }

    return HASHTABLE_UPDATE_ERR_NOT_FOUND;
}

HashTableUpsertResult hashtable_upsert(HashTable *table, const char *key,
                                     void *value) {
    if (hashtable_update(table, key, value) == HASHTABLE_UPDATE_OK) {
        return HASHTABLE_UPSERT_UPDATED;
    }

    HashTableInsertResult insert_result = _hashtable_insert(table, key, value);
    if (insert_result == HASHTABLE_OK) {
        return HASHTABLE_UPSERT_INSERTED;
    } else {
        return HASHTABLE_UPSERT_ERR_OOM;
    }
}

void hashtable_clear(HashTable *table) { table->head = NULL; }

HashTableIter hashtable_iter(HashTable *table) {
    HashTableIter it = {0};
    it.next = table->head;
    return it;
}
bool hashtable_iter_next(HashTableIter *it, const char **key, void **value) {
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
