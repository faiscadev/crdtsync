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

.PHONY: test-scalar
test-scalar: string.c scalar.c test_scalar.c test_util.h
	$(CC) $(CFLAGS) -o test_scalar string.c scalar.c test_scalar.c
	./test_scalar

.PHONY: test-register
test-register: arena.c string.c scalar.c register.c test_register.c test_util.h
	$(CC) $(CFLAGS) -o test_register arena.c string.c scalar.c register.c test_register.c
	./test_register

.PHONY: test-clientid
test-clientid: string.c clientid.c test_clientid.c test_util.h
	$(CC) $(CFLAGS) -o test_clientid string.c clientid.c test_clientid.c
	./test_clientid

.PHONY: test
test: test-hashtable test-string test-counter test-scalar test-register test-clientid
