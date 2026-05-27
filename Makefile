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
