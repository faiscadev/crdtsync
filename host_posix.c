// POSIX implementation of the host seam (host.h).
//
// Clock:   clock_gettime(CLOCK_REALTIME, ...) from <time.h>.
// Entropy: read from /dev/urandom — works on every POSIX system (Linux,
//          macOS, the BSDs) without feature-test macros or glibc version
//          sniffing.

#include "host.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
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
        abort();
    }
    size_t got = 0;
    while (got < n) {
        ssize_t r = read(fd, buf + got, n - got);
        if (r <= 0) {
            close(fd);
            abort();
        }
        got += (size_t)r;
    }
    close(fd);
}

_Noreturn void host_abort(const char *reason) {
    fprintf(stderr, "host_abort: %s\n", reason ? reason : "(no reason)");
    abort();
}
