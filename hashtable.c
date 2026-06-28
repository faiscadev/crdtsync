#include "hashtable.h"
#include "host.h"
#include "string.h"

const char *hashtable_insert_result_name(HashTableInsertResult r) {
    switch (r) {
    case HASHTABLE_OK:
        return "OK";
    case HASHTABLE_ERR_OOM:
        return "OOM";
    case HASHTABLE_ERR_KEY_EXISTS:
        return "KEY_EXISTS";
    }
}

typedef struct HashTableNode {
    void *key;
    size_t key_len;
    void *value;
    HashTableNode *next;
} HashTableNode;

HashTable *hashtable_create(void) {
    HashTable *table = host_malloc(sizeof(HashTable));
    if (!table) {
        return NULL;
    }

    table->head = NULL;
    return table;
}

void hashtable_destroy(HashTable *table) {
    if (table == NULL) {
        return;
    }
    HashTableNode *n = table->head;
    while (n) {
        HashTableNode *next = n->next;
        host_free(n->key);
        host_free(n);
        n = next;
    }
    host_free(table);
}

static HashTableInsertResult _hashtable_insert(HashTable *table,
                                               const void *key, size_t key_len,
                                               void *value) {
    HashTableNode *node = host_malloc(sizeof(HashTableNode));
    if (!node) {
        return HASHTABLE_ERR_OOM;
    }

    node->key = host_malloc(key_len);
    if (!node->key) {
        host_free(node);
        return HASHTABLE_ERR_OOM;
    }

    memcpy(node->key, key, key_len);
    node->value = value;
    node->key_len = key_len;

    // cons into head of list
    node->next = table->head;
    table->head = node;

    return HASHTABLE_OK;
}

HashTableInsertResult hashtable_insert(HashTable *table, const void *key,
                                       size_t key_len, void *value) {
    if (hashtable_get(table, key, key_len, NULL)) {
        return HASHTABLE_ERR_KEY_EXISTS;
    }

    return _hashtable_insert(table, key, key_len, value);
}

bool hashtable_get(HashTable *table, const void *key, size_t key_len,
                   void **out) {
    HashTableIter it = hashtable_iter(table);
    const void *k;
    void *v;
    size_t kl;
    while (hashtable_iter_next(&it, &k, &kl, &v)) {
        if (kl == key_len && memcmp(k, key, key_len) == 0) {
            if (out) {
                *out = v;
            }
            return true;
        }
    }
    return false;
}

HashTableRemoveResult hashtable_remove(HashTable *table, const void *key,
                                       size_t key_len) {
    HashTableNode *prev = NULL;
    for (HashTableNode *n = table->head; n; prev = n, n = n->next) {
        if (n->key_len == key_len && memcmp(n->key, key, key_len) == 0) {
            if (prev) {
                prev->next = n->next;
            } else {
                table->head = n->next;
            }

            host_free(n->key);
            host_free(n);

            return HASHTABLE_REMOVE_OK;
        }
    }

    return HASHTABLE_REMOVE_ERR_NOT_FOUND;
}

HashTableUpdateResult hashtable_update(HashTable *table, const void *key,
                                       size_t key_len, void *value) {
    for (HashTableNode *n = table->head; n; n = n->next) {
        if (n->key_len == key_len && memcmp(n->key, key, key_len) == 0) {
            n->value = value;
            return HASHTABLE_UPDATE_OK;
        }
    }

    return HASHTABLE_UPDATE_ERR_NOT_FOUND;
}

HashTableUpsertResult hashtable_upsert(HashTable *table, const void *key,
                                       size_t key_len, void *value) {
    if (hashtable_update(table, key, key_len, value) == HASHTABLE_UPDATE_OK) {
        return HASHTABLE_UPSERT_UPDATED;
    }

    HashTableInsertResult insert_result =
        _hashtable_insert(table, key, key_len, value);
    if (insert_result == HASHTABLE_OK) {
        return HASHTABLE_UPSERT_INSERTED;
    } else {
        return HASHTABLE_UPSERT_ERR_OOM;
    }
}

void hashtable_clear(HashTable *table) {
    HashTableNode *n = table->head;
    while (n) {
        HashTableNode *next = n->next;
        host_free(n->key);
        host_free(n);
        n = next;
    }

    table->head = NULL;
}

HashTableIter hashtable_iter(HashTable *table) {
    HashTableIter it = {0};
    it.next = table->head;
    return it;
}
bool hashtable_iter_next(HashTableIter *it, const void **key, size_t *key_len,
                         void **value) {
    if (it->next == NULL) {
        return false;
    }

    if (key) {
        *key = it->next->key;
    }

    if (key_len) {
        *key_len = it->next->key_len;
    }

    if (value) {
        *value = it->next->value;
    }

    it->next = it->next->next;

    return true;
}
