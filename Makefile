CC = clang
CFLAGS = -Wall -Wextra -g

.PHONY: test-hashtable
test-hashtable: arena.c string.c hashtable.c test_hashtable.c test_util.h
	$(CC) $(CFLAGS) -o test_hashtable arena.c string.c hashtable.c test_hashtable.c
	./test_hashtable

.PHONY: test-string
test-string: string.c test_string.c test_util.h
	$(CC) $(CFLAGS) -fno-builtin -o test_string string.c test_string.c
	./test_string

.PHONY: test-counter
test-counter: arena.c string.c hashtable.c counter.c test_counter.c test_util.h
	$(CC) $(CFLAGS) -o test_counter arena.c string.c hashtable.c counter.c test_counter.c
	./test_counter

.PHONY: test
test: test-hashtable test-string test-counter
