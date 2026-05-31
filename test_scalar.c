// Native unit tests for the Scalar value type. Compile with system clang,
// link string.c + scalar.c. Run the binary.
//
// Expected API (you implement in scalar.h + scalar.c):
//
//   typedef enum {
//       SCALAR_NULL,
//       SCALAR_BOOL,
//       SCALAR_INT,
//       SCALAR_STRING,
//   } ScalarKind;
//
//   typedef struct Scalar {
//       ScalarKind kind;
//       union {
//           bool    b;
//           int64_t i;
//           struct { const uint8_t *bytes; size_t len; } s;
//       } as;
//   } Scalar;
//
//   Scalar scalar_null(void);
//   Scalar scalar_bool(bool b);
//   Scalar scalar_int(int64_t i);
//   Scalar scalar_string(const uint8_t *bytes, size_t len);
//   bool   scalar_eq(Scalar a, Scalar b);
//
// Notes:
//   - Pass by value (~24 bytes).
//   - SCALAR_STRING bytes are borrowed at the boundary; whoever stores a
//     Scalar long-term dups into its own arena.
//   - scalar_eq compares by KIND first; equal kind compares the inner value
//     (bytes+len memcmp for strings, binary-safe).
//   - Cross-kind comparison is always false, even for "obvious" equivalents
//     (e.g. scalar_int(0) != scalar_bool(false)). The tag matters.

#include "scalar.h"
#include "test_util.h"
#include <stdint.h>

// --- constructors set the right kind/value ---

TEST(null_has_null_kind) {
    Scalar s = scalar_null();
    ASSERT_EQ(s.kind, SCALAR_NULL);
}

TEST(bool_true_kind_and_value) {
    Scalar s = scalar_bool(true);
    ASSERT_EQ(s.kind, SCALAR_BOOL);
    ASSERT_EQ(s.as.b, true);
}

TEST(bool_false_kind_and_value) {
    Scalar s = scalar_bool(false);
    ASSERT_EQ(s.kind, SCALAR_BOOL);
    ASSERT_EQ(s.as.b, false);
}

TEST(int_kind_and_value) {
    Scalar s = scalar_int(42);
    ASSERT_EQ(s.kind, SCALAR_INT);
    ASSERT_EQ(s.as.i, 42);
}

TEST(int_negative) {
    Scalar s = scalar_int(-7);
    ASSERT_EQ(s.kind, SCALAR_INT);
    ASSERT_EQ(s.as.i, -7);
}

TEST(int_extremes) {
    Scalar mn = scalar_int(INT64_MIN);
    Scalar mx = scalar_int(INT64_MAX);
    ASSERT_EQ(mn.as.i, INT64_MIN);
    ASSERT_EQ(mx.as.i, INT64_MAX);
}

TEST(string_kind_bytes_len) {
    const uint8_t *bytes = (const uint8_t *)"hello";
    Scalar s = scalar_string(bytes, 5);
    ASSERT_EQ(s.kind, SCALAR_STRING);
    ASSERT(s.as.s.bytes == bytes); // borrowed pointer, not copied
    ASSERT_EQ(s.as.s.len, 5);
}

TEST(string_empty_is_valid) {
    Scalar s = scalar_string(NULL, 0);
    ASSERT_EQ(s.kind, SCALAR_STRING);
    ASSERT_EQ(s.as.s.len, 0);
}

// --- equality: same kind, equal payload ---

TEST(null_eq_null) { ASSERT(scalar_eq(scalar_null(), scalar_null()) == true); }

TEST(bool_eq_same) {
    ASSERT(scalar_eq(scalar_bool(true), scalar_bool(true)) == true);
    ASSERT(scalar_eq(scalar_bool(false), scalar_bool(false)) == true);
}

TEST(bool_neq_different) {
    ASSERT(scalar_eq(scalar_bool(true), scalar_bool(false)) == false);
}

TEST(int_eq_same) {
    ASSERT(scalar_eq(scalar_int(42), scalar_int(42)) == true);
    ASSERT(scalar_eq(scalar_int(0), scalar_int(0)) == true);
    ASSERT(scalar_eq(scalar_int(-1), scalar_int(-1)) == true);
}

TEST(int_neq_different) {
    ASSERT(scalar_eq(scalar_int(42), scalar_int(43)) == false);
    ASSERT(scalar_eq(scalar_int(1), scalar_int(-1)) == false);
}

TEST(string_eq_same_content) {
    Scalar a = scalar_string((const uint8_t *)"abc", 3);
    Scalar b = scalar_string((const uint8_t *)"abc", 3);
    ASSERT(scalar_eq(a, b) == true);
}

TEST(string_neq_different_content_same_length) {
    Scalar a = scalar_string((const uint8_t *)"abc", 3);
    Scalar b = scalar_string((const uint8_t *)"abd", 3);
    ASSERT(scalar_eq(a, b) == false);
}

TEST(string_neq_different_length_same_prefix) {
    Scalar a = scalar_string((const uint8_t *)"ab", 2);
    Scalar b = scalar_string((const uint8_t *)"abc", 3);
    ASSERT(scalar_eq(a, b) == false);
}

TEST(string_empty_eq_empty) {
    Scalar a = scalar_string(NULL, 0);
    Scalar b = scalar_string((const uint8_t *)"", 0);
    ASSERT(scalar_eq(a, b) == true);
}

// Binary-safe: embedded NUL bytes are part of the value.
TEST(string_eq_with_embedded_nul) {
    uint8_t a[3] = {0x01, 0x00, 0x02};
    uint8_t b[3] = {0x01, 0x00, 0x02};
    uint8_t c[3] = {0x01, 0x00, 0x03};
    ASSERT(scalar_eq(scalar_string(a, 3), scalar_string(b, 3)) == true);
    ASSERT(scalar_eq(scalar_string(a, 3), scalar_string(c, 3)) == false);
}

// --- cross-kind: never equal, even for "obvious" coincidences ---

TEST(null_neq_bool_false) {
    ASSERT(scalar_eq(scalar_null(), scalar_bool(false)) == false);
}

TEST(null_neq_int_zero) {
    ASSERT(scalar_eq(scalar_null(), scalar_int(0)) == false);
}

TEST(bool_false_neq_int_zero) {
    ASSERT(scalar_eq(scalar_bool(false), scalar_int(0)) == false);
}

TEST(bool_true_neq_int_one) {
    ASSERT(scalar_eq(scalar_bool(true), scalar_int(1)) == false);
}

TEST(int_neq_string) {
    ASSERT(scalar_eq(scalar_int(42), scalar_string((const uint8_t *)"42", 2)) ==
           false);
}

TEST(string_neq_null) {
    ASSERT(scalar_eq(scalar_string(NULL, 0), scalar_null()) == false);
}

int main(void) {
    RUN(null_has_null_kind);
    RUN(bool_true_kind_and_value);
    RUN(bool_false_kind_and_value);
    RUN(int_kind_and_value);
    RUN(int_negative);
    RUN(int_extremes);
    RUN(string_kind_bytes_len);
    RUN(string_empty_is_valid);

    RUN(null_eq_null);
    RUN(bool_eq_same);
    RUN(bool_neq_different);
    RUN(int_eq_same);
    RUN(int_neq_different);
    RUN(string_eq_same_content);
    RUN(string_neq_different_content_same_length);
    RUN(string_neq_different_length_same_prefix);
    RUN(string_empty_eq_empty);
    RUN(string_eq_with_embedded_nul);

    RUN(null_neq_bool_false);
    RUN(null_neq_int_zero);
    RUN(bool_false_neq_int_zero);
    RUN(bool_true_neq_int_one);
    RUN(int_neq_string);
    RUN(string_neq_null);

    TEST_SUMMARY();
}
