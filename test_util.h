#ifndef _CRDT_TEST_UTIL_H
#define _CRDT_TEST_UTIL_H

#include <stdio.h>
#include <unistd.h>

static int test_count = 0;
static int test_fail_count = 0;

// Per-test failure state, captured by ASSERT* and reported by RUN so that the
// test name and the failure detail print together, in order.
static int test_cur_failed = 0;
static char test_fail_msg[512];

// Colors only when stdout is a terminal; piping to a file or CI stays clean.
#define TC_RED (isatty(1) ? "\x1b[31m" : "")
#define TC_GREEN (isatty(1) ? "\x1b[32m" : "")
#define TC_DIM (isatty(1) ? "\x1b[2m" : "")
#define TC_BOLD (isatty(1) ? "\x1b[1m" : "")
#define TC_RESET (isatty(1) ? "\x1b[0m" : "")

#define TEST(name) static void name(void)

#define RUN(name)                                                              \
    do {                                                                       \
        test_count++;                                                          \
        test_cur_failed = 0;                                                   \
        test_fail_msg[0] = '\0';                                               \
        name();                                                                \
        if (test_cur_failed) {                                                 \
            test_fail_count++;                                                 \
            printf("%s%s FAIL %s %s\n        %s\n", TC_RED, TC_BOLD, TC_RESET,  \
                   #name, test_fail_msg);                                      \
        } else {                                                               \
            printf("%s ok %s   %s\n", TC_GREEN, TC_RESET, #name);              \
        }                                                                      \
    } while (0)

#define ASSERT(cond)                                                           \
    do {                                                                       \
        if (!(cond)) {                                                         \
            test_cur_failed = 1;                                               \
            snprintf(test_fail_msg, sizeof(test_fail_msg),                     \
                     "%s:%d  ASSERT(%s)", __FILE__, __LINE__, #cond);          \
            return;                                                            \
        }                                                                      \
    } while (0)

#define ASSERT_EQ(a, b)                                                        \
    do {                                                                       \
        long long _a = (long long)(a);                                         \
        long long _b = (long long)(b);                                         \
        if (_a != _b) {                                                        \
            test_cur_failed = 1;                                               \
            snprintf(test_fail_msg, sizeof(test_fail_msg),                     \
                     "%s:%d  ASSERT_EQ(%s, %s)  %lld != %lld", __FILE__,       \
                     __LINE__, #a, #b, _a, _b);                                \
            return;                                                            \
        }                                                                      \
    } while (0)

#define TEST_SUMMARY()                                                         \
    do {                                                                       \
        int _passed = test_count - test_fail_count;                            \
        if (test_fail_count == 0) {                                            \
            printf("\n%s%s all %d passed %s\n", TC_GREEN, TC_BOLD, test_count, \
                   TC_RESET);                                                  \
        } else {                                                               \
            printf("\n%s%s %d failed %s, %d passed (of %d)\n", TC_RED,         \
                   TC_BOLD, test_fail_count, TC_RESET, _passed, test_count);   \
        }                                                                      \
        return test_fail_count;                                                \
    } while (0)

#endif // _CRDT_TEST_UTIL_H
