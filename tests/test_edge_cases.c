/*
 * test_edge_cases.c - boundary conditions and special float values
 */
#include "unity.h"
#include "gorilla.h"
#include <stdlib.h>
#include <string.h>
#include <math.h>

void setUp(void)    {}
void tearDown(void) {}

#define BASE_TS 1700000000LL

void test_single_point(void) {
    int64_t ts  = BASE_TS + 30;
    double  val = 99.5;
    size_t  cap = gorilla_max_compressed_size(1);
    uint8_t *buf = malloc(cap);

    size_t bytes = gorilla_compress(BASE_TS, &ts, &val, 1, buf, cap);
    TEST_ASSERT_GREATER_THAN_size_t(0, bytes);

    int64_t ts_out; double val_out; size_t n_out;
    TEST_ASSERT_EQUAL_INT(0,
        gorilla_decompress(buf, bytes, &ts_out, &val_out, 1, &n_out));
    TEST_ASSERT_EQUAL_size_t(1, n_out);
    TEST_ASSERT_EQUAL_INT64(ts,  ts_out);
    TEST_ASSERT_EQUAL_DOUBLE(val, val_out);
    free(buf);
}

void test_two_points(void) {
    int64_t ts[2]   = { BASE_TS + 60, BASE_TS + 120 };
    double  vals[2] = { 1.0, 2.0 };
    size_t  cap = gorilla_max_compressed_size(2);
    uint8_t *buf = malloc(cap);

    size_t bytes = gorilla_compress(BASE_TS, ts, vals, 2, buf, cap);
    int64_t ts_out[2]; double val_out[2]; size_t n_out;
    gorilla_decompress(buf, bytes, ts_out, val_out, 2, &n_out);
    TEST_ASSERT_EQUAL_size_t(2, n_out);
    TEST_ASSERT_EQUAL_INT64(ts[0],   ts_out[0]);
    TEST_ASSERT_EQUAL_INT64(ts[1],   ts_out[1]);
    TEST_ASSERT_EQUAL_DOUBLE(vals[0], val_out[0]);
    TEST_ASSERT_EQUAL_DOUBLE(vals[1], val_out[1]);
    free(buf);
}

void test_positive_infinity_preserved(void) {
    double inf = (double)(1.0 / 0.0);
    int64_t ts[2]   = { BASE_TS + 60, BASE_TS + 120 };
    double  vals[2] = { 1.0, inf };
    size_t  cap = gorilla_max_compressed_size(2);
    uint8_t *buf = malloc(cap);

    size_t bytes = gorilla_compress(BASE_TS, ts, vals, 2, buf, cap);
    int64_t ts_out[2]; double val_out[2]; size_t n_out;
    gorilla_decompress(buf, bytes, ts_out, val_out, 2, &n_out);

    uint64_t expected, actual;
    memcpy(&expected, &inf,        8);
    memcpy(&actual,   &val_out[1], 8);
    TEST_ASSERT_EQUAL_UINT64(expected, actual);
    free(buf);
}

void test_nan_preserved_bit_exact(void) {
    /* NaN != NaN by IEEE 754, so compare bit patterns */
    double nan_val = (double)(0.0 / 0.0);
    int64_t ts[2]   = { BASE_TS + 60, BASE_TS + 120 };
    double  vals[2] = { 1.0, nan_val };
    size_t  cap = gorilla_max_compressed_size(2);
    uint8_t *buf = malloc(cap);

    size_t bytes = gorilla_compress(BASE_TS, ts, vals, 2, buf, cap);
    int64_t ts_out[2]; double val_out[2]; size_t n_out;
    gorilla_decompress(buf, bytes, ts_out, val_out, 2, &n_out);

    uint64_t expected, actual;
    memcpy(&expected, &nan_val,    8);
    memcpy(&actual,   &val_out[1], 8);
    TEST_ASSERT_EQUAL_UINT64(expected, actual);
    free(buf);
}

void test_negative_zero_preserved(void) {
    double neg_zero = -0.0;
    int64_t ts[2]   = { BASE_TS + 60, BASE_TS + 120 };
    double  vals[2] = { 1.0, neg_zero };
    size_t  cap = gorilla_max_compressed_size(2);
    uint8_t *buf = malloc(cap);

    size_t bytes = gorilla_compress(BASE_TS, ts, vals, 2, buf, cap);
    int64_t ts_out[2]; double val_out[2]; size_t n_out;
    gorilla_decompress(buf, bytes, ts_out, val_out, 2, &n_out);

    uint64_t expected, actual;
    memcpy(&expected, &neg_zero,   8);
    memcpy(&actual,   &val_out[1], 8);
    TEST_ASSERT_EQUAL_UINT64(expected, actual);
    free(buf);
}

void test_maximum_first_delta(void) {
    /* 14-bit first delta max = 16383 seconds ≈ 4.5 hours */
    int64_t ts  = BASE_TS + 16383;
    double  val = 1.0;
    size_t  cap = gorilla_max_compressed_size(1);
    uint8_t *buf = malloc(cap);

    size_t bytes = gorilla_compress(BASE_TS, &ts, &val, 1, buf, cap);
    TEST_ASSERT_GREATER_THAN_size_t(0, bytes);

    int64_t ts_out; double val_out; size_t n_out;
    gorilla_decompress(buf, bytes, &ts_out, &val_out, 1, &n_out);
    TEST_ASSERT_EQUAL_INT64(ts, ts_out);
    free(buf);
}

void test_first_delta_out_of_range_returns_error(void) {
    /* Delta > 16383 cannot be stored in 14 bits → must fail gracefully */
    int64_t ts  = BASE_TS + 16384;
    double  val = 1.0;
    size_t  cap = gorilla_max_compressed_size(1);
    uint8_t *buf = malloc(cap);
    memset(buf, 0, cap);

    size_t bytes = gorilla_compress(BASE_TS, &ts, &val, 1, buf, cap);
    TEST_ASSERT_EQUAL_size_t(0, bytes);  /* must signal failure */
    free(buf);
}

/* -------------------------------------------------------------------------
 * Runner
 * ------------------------------------------------------------------------- */
int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_single_point);
    RUN_TEST(test_two_points);
    RUN_TEST(test_positive_infinity_preserved);
    RUN_TEST(test_nan_preserved_bit_exact);
    RUN_TEST(test_negative_zero_preserved);
    RUN_TEST(test_maximum_first_delta);
    RUN_TEST(test_first_delta_out_of_range_returns_error);
    return UNITY_END();
}
