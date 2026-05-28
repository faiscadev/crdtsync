#ifndef _CRDT_STRING_H
#define _CRDT_STRING_H

#include <stddef.h>

int strcmp(const char *s1, const char *s2);

unsigned long strlen(const char *str);

char *strcpy(char *dst, const char *src);

void *memset(void *ptr, int value, size_t num);

void *memcpy(void *dst, const void *src, size_t num);

int memcmp(const void *ptr1, const void *ptr2, size_t num);

#endif // _CRDT_STRING_H
