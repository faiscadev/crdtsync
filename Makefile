CC = clang
CFLAGS = -Wall -Wextra -g

SRC = $(wildcard *.c *.h)

# Pinned clang-format, installed into a local venv so `make fmt` uses the exact
# same version everywhere (local + CI) with zero setup.
CLANG_FORMAT_VERSION = 22.1.5
FMT_VENV = .venv-fmt
CLANG_FORMAT = $(FMT_VENV)/bin/clang-format

$(CLANG_FORMAT):
	python3 -m venv $(FMT_VENV)
	$(FMT_VENV)/bin/pip install --quiet clang-format==$(CLANG_FORMAT_VERSION)

.PHONY: fmt
fmt: $(CLANG_FORMAT)
	$(CLANG_FORMAT) -i $(SRC)

.PHONY: fmt-check
fmt-check: $(CLANG_FORMAT)
	$(CLANG_FORMAT) --dry-run --Werror $(SRC)

.PHONY: test-arena
test-arena: arena.c string.c host_posix.c test_arena.c test_util.h
	$(CC) $(CFLAGS) -o test_arena arena.c string.c host_posix.c test_arena.c
	./test_arena

.PHONY: test-hashtable
test-hashtable: arena.c string.c host_posix.c hashtable.c test_hashtable.c test_util.h
	$(CC) $(CFLAGS) -o test_hashtable arena.c string.c host_posix.c hashtable.c test_hashtable.c
	./test_hashtable

.PHONY: test-string
test-string: string.c test_string.c test_util.h
	$(CC) $(CFLAGS) -fno-builtin -o test_string string.c test_string.c
	./test_string

.PHONY: test-counter
test-counter: arena.c string.c hashtable.c clientid.c host_posix.c counter.c test_counter.c test_util.h
	$(CC) $(CFLAGS) -o test_counter arena.c string.c hashtable.c clientid.c host_posix.c counter.c test_counter.c
	./test_counter

.PHONY: test-scalar
test-scalar: arena.c string.c host_posix.c scalar.c test_scalar.c test_util.h
	$(CC) $(CFLAGS) -o test_scalar arena.c string.c host_posix.c scalar.c test_scalar.c
	./test_scalar

.PHONY: test-register
test-register: arena.c string.c clientid.c host_posix.c stamp.c scalar.c register.c test_register.c test_util.h
	$(CC) $(CFLAGS) -o test_register arena.c string.c clientid.c host_posix.c stamp.c scalar.c register.c test_register.c
	./test_register

.PHONY: test-map
test-map: arena.c string.c hashtable.c clientid.c host_posix.c stamp.c scalar.c map.c test_map.c test_util.h
	$(CC) $(CFLAGS) -o test_map arena.c string.c hashtable.c clientid.c host_posix.c stamp.c scalar.c map.c test_map.c
	./test_map

.PHONY: test-clientid
test-clientid: string.c clientid.c host_posix.c test_clientid.c test_util.h
	$(CC) $(CFLAGS) -o test_clientid string.c clientid.c host_posix.c test_clientid.c
	./test_clientid

.PHONY: test-stamp
test-stamp: string.c clientid.c host_posix.c stamp.c test_stamp.c test_util.h
	$(CC) $(CFLAGS) -o test_stamp string.c clientid.c host_posix.c stamp.c test_stamp.c
	./test_stamp

.PHONY: test
test: test-arena test-hashtable test-string test-counter test-scalar test-register test-clientid test-stamp test-map
