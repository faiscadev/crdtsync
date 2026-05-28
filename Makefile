CC = clang
CFLAGS = -Wall -Wextra -g

.PHONY: test-hashtable
test-hashtable: arena.c string.c hashtable.c test_hashtable.c test_util.h
	$(CC) $(CFLAGS) -o test_hashtable arena.c string.c hashtable.c test_hashtable.c
	./test_hashtable
