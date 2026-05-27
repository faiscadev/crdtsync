add: add.c
	clang --target=wasm32 \
		-O3 \
		-nostdlib \
		-Wl,--no-entry \
		-Wl,--export-all \
		-o add.wasm \
		add.c
