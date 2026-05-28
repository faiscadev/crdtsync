crdt.wasm: crdt.c
	clang --target=wasm32 \
		-O3 \
		-nostdlib \
		-Wl,--no-entry \
		-Wl,--export-all \
		-Wl,--export-memory \
		-o crdt.wasm \
		crdt.c

.PHONY: smoke
smoke: crdt.wasm smoke.js
	node smoke.js

.PHONY: test
test: crdt.wasm
	node --test 'tests/**/*.test.js'

.PHONY: test-hashtabl
test-hashtabl: arena.c string.c hashtabl.c test_hashtabl.c test_util.h
	clang -Wall -Wextra -g -o test_hashtabl arena.c string.c hashtabl.c test_hashtabl.c
	./test_hashtabl
