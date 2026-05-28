CC = clang
CFLAGS = -Wall -Wextra -g

.PHONY: test-hashtabl
test-hashtabl: arena.c string.c hashtabl.c test_hashtabl.c test_util.h
	$(CC) $(CFLAGS) -o test_hashtabl arena.c string.c hashtabl.c test_hashtabl.c
	./test_hashtabl
