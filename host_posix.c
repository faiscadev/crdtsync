// POSIX implementation of the host seam (host.h).
//
// Clock:   clock_gettime(CLOCK_REALTIME, ...) from <time.h>.
// Entropy: read from /dev/urandom — works on every POSIX system (Linux,
//          macOS, the BSDs) without feature-test macros or glibc version
//          sniffing.

#include "host.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

uint64_t host_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)(ts.tv_nsec / 1000000);
}

void host_fill_entropy(uint8_t *buf, size_t n) {
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd < 0) {
        // Catastrophic — host_fill_entropy is documented as infallible.
        host_abortf("host_fill_entropy: open(/dev/urandom) failed: %s",
                    strerror(errno));
    }
    size_t got = 0;
    while (got < n) {
        ssize_t r = read(fd, buf + got, n - got);
        if (r <= 0) {
            int saved_errno = errno;
            close(fd);
            host_abortf(
                "host_fill_entropy: read(/dev/urandom) returned %zd (errno %d "
                "%s) after %zu of %zu bytes",
                r, saved_errno, r < 0 ? strerror(saved_errno) : "EOF", got, n);
        }
        got += (size_t)r;
    }
    close(fd);
}

_Noreturn void host_abort(const char *reason) {
    fprintf(stderr, "host_abort: %s\n", reason ? reason : "(no reason)");
    abort();
}

_Noreturn void host_abortf(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    fputs("host_abort: ", stderr);
    vfprintf(stderr, fmt, args);
    fputc('\n', stderr);
    va_end(args);
    abort();
}
