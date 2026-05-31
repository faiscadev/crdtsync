#ifndef _CRDT_HOST_H
#define _CRDT_HOST_H

// Host-injectable platform primitives.
//
// Anything that depends on a clock or on cryptographic randomness goes
// through this seam. The CRDT core stays platform-agnostic by calling
// these declarations; each target build links exactly one implementation:
//
//   - host_posix.c   (Linux, macOS, BSDs) — clock_gettime + /dev/urandom
//   - host_wasm.c    (WebAssembly)        — imports Date.now /
//   crypto.getRandomValues
//   - host_windows.c (Windows MSVC)       — timespec_get + BCryptGenRandom
//
// Functions never fail (entropy / clock sources at this level are
// assumed available; per-target impls abort on catastrophic failure).
// Callers do not need to handle errors.

#include <stddef.h>
#include <stdint.h>

// Current wall-clock time in milliseconds since the Unix epoch.
uint64_t host_now_ms(void);

// Fill `n` bytes of `buf` with cryptographically suitable randomness.
void host_fill_entropy(uint8_t *buf, size_t n);

#endif // _CRDT_HOST_H
