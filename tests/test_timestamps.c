/*
 * test_timestamps.c - Timestamp delta-of-delta encoding (paper §4.1.1)
 */
#include "unity.h"
#include "gorilla.h"
#include <stdlib.h>

void setUp(void)    {}
void tearDown(void) {}

#define BASE_TS  1700000000LL

/* Helper: round-trip n points, return compressed byte count. */
static size_t compress_and_verify(const int64_t *ts, const double *vals,
                                  int n, const char *ctx) {
    size_t cap = gorilla_max_compressed_size((size_t)n);
    uint8_t *buf = malloc(cap);
    TEST_ASSERT_NOT_NULL_MESSAGE(buf, ctx);

    size_t bytes = gorilla_compress(BASE_TS, ts, vals, (size_t)n, buf, cap);
    TEST_ASSERT_GREATER_THAN_size_t_MESSAGE(0, bytes, ctx);

    int64_t *ts_out  = malloc((size_t)n * sizeof(*ts_out));
    double  *val_out = malloc((size_t)n * sizeof(*val_out));
    size_t n_out = 0;
    TEST_ASSERT_EQUAL_INT_MESSAGE(0,
        gorilla_decompress(buf, bytes, ts_out, val_out, (size_t)n, &n_out), ctx);
    TEST_ASSERT_EQUAL_size_t_MESSAGE((size_t)n, n_out, ctx);

    for (int i = 0; i < n; i++) {
        TEST_ASSERT_EQUAL_INT64_MESSAGE(ts[i],   ts_out[i],  ctx);
        TEST_ASSERT_EQUAL_DOUBLE_MESSAGE(vals[i], val_out[i], ctx);
    }

    free(ts_out); free(val_out); free(buf);
    return bytes;
}

/* -------------------------------------------------------------------------
 * Tests
 * ------------------------------------------------------------------------- */

void test_regular_60s_interval_compresses_well(void) {
    /* Paper §4.1.1: ~96% of DoDs are 0 for regular series → strong compression */
    const int N = 100;
    int64_t ts[100];
    double  vals[100];
    for (int i = 0; i < N; i++) { ts[i] = BASE_TS + i * 60; vals[i] = 1.0; }

    size_t bytes = compress_and_verify(ts, vals, N, "regular 60s");
    size_t raw   = (size_t)N * 16;
    /* Should achieve much better than 4x on regular + constant data */
    TEST_ASSERT_LESS_THAN_size_t(raw / 4, bytes);
}

void test_regular_15s_interval(void) {
    /* Paper uses 15s as the standard ODS granularity */
    const int N = 80;
    int64_t ts[80];
    double  vals[80];
    for (int i = 0; i < N; i++) { ts[i] = BASE_TS + i * 15; vals[i] = 0.5; }
    compress_and_verify(ts, vals, N, "regular 15s");
}

void test_irregular_timestamps_with_jitter(void) {
    /* Occasional ±1s jitter: DoD alternates between -1, 0, 1 → 9-bit bucket */
    const int N = 10;
    int64_t ts[10] = {
        BASE_TS + 60,  BASE_TS + 121, BASE_TS + 180, BASE_TS + 241,
        BASE_TS + 299, BASE_TS + 361, BASE_TS + 420, BASE_TS + 481,
        BASE_TS + 540, BASE_TS + 602
    };
    double vals[10];
    for (int i = 0; i < N; i++) vals[i] = (double)i;
    compress_and_verify(ts, vals, N, "jitter ±1s");
}

void test_missing_datapoints(void) {
    /* A missing point doubles the delta; DoD = ±original_delta.
     * Paper example: deltas 60,60,121,59 → DoDs 0,61,-62 (fits 7-bit bucket) */
    const int N = 6;
    int64_t ts[6] = {
        BASE_TS + 60, BASE_TS + 120, BASE_TS + 180,
        /* missing 240 */
        BASE_TS + 300, BASE_TS + 360, BASE_TS + 420
    };
    double vals[6] = {1,2,3,4,5,6};
    compress_and_verify(ts, vals, N, "missing point");
}

void test_large_timestamp_gap_uses_32bit_bucket(void) {
    /* DoD > 2048 forces the 32-bit fallback bucket (paper §4.1.1 §2f) */
    const int N = 3;
    int64_t ts[3] = { BASE_TS + 60, BASE_TS + 120, BASE_TS + 10000 };
    double  vals[3] = {1.0, 2.0, 3.0};
    compress_and_verify(ts, vals, N, "large gap 32-bit bucket");
}

/* -------------------------------------------------------------------------
 * Runner
 * ------------------------------------------------------------------------- */
int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_regular_60s_interval_compresses_well);
    RUN_TEST(test_regular_15s_interval);
    RUN_TEST(test_irregular_timestamps_with_jitter);
    RUN_TEST(test_missing_datapoints);
    RUN_TEST(test_large_timestamp_gap_uses_32bit_bucket);
    return UNITY_END();
}
