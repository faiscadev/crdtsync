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

// Abort the process — programmer-error escape hatch for invariant violations
// (e.g. passing an out-of-range mark to arena_restore). `reason` is a static
// string suitable for logging. Never returns.
_Noreturn void host_abort(const char *reason);

// printf-style variant. Per-target impls must support at minimum %s, %d, %u
// and %zu so primitives can interpolate enum names, sizes, and ids in their
// abort messages. Never returns.
_Noreturn void host_abortf(const char *fmt, ...);

void *host_malloc(size_t size);

void host_free(void *ptr);

void *host_realloc(void *ptr, size_t new_size);

#endif // _CRDT_HOST_H
