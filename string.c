#include "string.h"
#include <stdint.h>

int strcmp(const char *s1, const char *s2) {
    while (*s1 && (*s1 == *s2)) {
        s1++;
        s2++;
    }
    return *(unsigned char *)s1 - *(unsigned char *)s2;
}

unsigned long strlen(const char *str) {
    unsigned long len = 0;
    while (*str) {
        len++;
        str++;
    }
    return len;
}

char *strcpy(char *dst, const char *src) {
    char *orig = dst;
    while (*src) {
        *dst = *src;
        dst++;
        src++;
    }
    *dst = '\0';
    return orig;
}

void *memset(void *ptr, int value, size_t num) {
    uint8_t *p = (uint8_t *)ptr;
    for (size_t i = 0; i < num; i++) {
        p[i] = (uint8_t)value;
    }
    return ptr;
}

void *memcpy(void *dst, const void *src, size_t num) {
    uint8_t *d = (uint8_t *)dst;
    const uint8_t *s = (const uint8_t *)src;
    for (size_t i = 0; i < num; i++) {
        d[i] = s[i];
    }
    return dst;
}

int memcmp(const void *ptr1, const void *ptr2, size_t num) {
    const uint8_t *p1 = (const uint8_t *)ptr1;
    const uint8_t *p2 = (const uint8_t *)ptr2;
    for (size_t i = 0; i < num; i++) {
        if (p1[i] != p2[i]) {
            return p1[i] - p2[i];
        }
    }
    return 0;
}
