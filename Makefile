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
test-counter: arena.c string.c hashtable.c clientid.c elementid.c uuid.c sha1.c host_posix.c counter.c test_counter.c test_util.h
	$(CC) $(CFLAGS) -o test_counter arena.c string.c hashtable.c clientid.c elementid.c uuid.c sha1.c host_posix.c counter.c test_counter.c
	./test_counter

.PHONY: test-scalar
test-scalar: arena.c string.c host_posix.c scalar.c test_scalar.c test_util.h
	$(CC) $(CFLAGS) -o test_scalar arena.c string.c host_posix.c scalar.c test_scalar.c
	./test_scalar

.PHONY: test-register
test-register: arena.c string.c clientid.c elementid.c uuid.c sha1.c host_posix.c stamp.c scalar.c register.c test_register.c test_util.h
	$(CC) $(CFLAGS) -o test_register arena.c string.c clientid.c elementid.c uuid.c sha1.c host_posix.c stamp.c scalar.c register.c test_register.c
	./test_register

.PHONY: test-map
test-map: arena.c string.c hashtable.c clientid.c elementid.c uuid.c sha1.c host_posix.c stamp.c scalar.c register.c counter.c element.c map.c test_map.c test_util.h
	$(CC) $(CFLAGS) -o test_map arena.c string.c hashtable.c clientid.c elementid.c uuid.c sha1.c host_posix.c stamp.c scalar.c register.c counter.c element.c map.c test_map.c
	./test_map

.PHONY: test-element
test-element: arena.c string.c hashtable.c clientid.c elementid.c uuid.c sha1.c host_posix.c stamp.c scalar.c register.c counter.c map.c element.c test_element.c test_util.h
	$(CC) $(CFLAGS) -o test_element arena.c string.c hashtable.c clientid.c elementid.c uuid.c sha1.c host_posix.c stamp.c scalar.c register.c counter.c map.c element.c test_element.c
	./test_element

.PHONY: test-elementid
test-elementid: string.c clientid.c elementid.c uuid.c sha1.c host_posix.c test_elementid.c test_util.h
	$(CC) $(CFLAGS) -o test_elementid string.c clientid.c elementid.c uuid.c sha1.c host_posix.c test_elementid.c
	./test_elementid

.PHONY: test-sha1
test-sha1: sha1.c test_sha1.c test_util.h
	$(CC) $(CFLAGS) -o test_sha1 sha1.c test_sha1.c
	./test_sha1

# Exercises sha1.c's runtime endian-detection fallback by forcing the
# SHA1_USE_RUNTIME_ENDIAN path at compile time. Independent of whether the
# host's system headers happen to predefine BYTE_ORDER. Must produce
# byte-identical digests to the default build.
.PHONY: test-sha1-runtime-endian
test-sha1-runtime-endian: sha1.c test_sha1.c test_util.h
	$(CC) $(CFLAGS) -DSHA1_USE_RUNTIME_ENDIAN -o test_sha1_runtime_endian sha1.c test_sha1.c
	./test_sha1_runtime_endian

.PHONY: test-uuid
test-uuid: uuid.c sha1.c test_uuid.c test_util.h
	$(CC) $(CFLAGS) -o test_uuid uuid.c sha1.c test_uuid.c
	./test_uuid

.PHONY: test-clientid
test-clientid: string.c clientid.c host_posix.c test_clientid.c test_util.h
	$(CC) $(CFLAGS) -o test_clientid string.c clientid.c host_posix.c test_clientid.c
	./test_clientid

.PHONY: test-stamp
test-stamp: string.c clientid.c host_posix.c stamp.c test_stamp.c test_util.h
	$(CC) $(CFLAGS) -o test_stamp string.c clientid.c host_posix.c stamp.c test_stamp.c
	./test_stamp

.PHONY: test
test: test-arena test-hashtable test-string test-counter test-scalar test-register test-clientid test-stamp test-map test-sha1 test-sha1-runtime-endian test-uuid test-elementid test-element
