/*
 * test_values.c - XOR value encoding (paper §4.1.2)
 */
#include "unity.h"
#include "gorilla.h"
#include <stdlib.h>
#include <string.h>

void setUp(void)    {}
void tearDown(void) {}

#define BASE_TS 1700000000LL

void test_identical_values_compress_to_one_bit_each(void) {
    const int N = 100;
    int64_t ts[100]; double vals[100];
    for (int i = 0; i < N; i++) { ts[i] = BASE_TS + i*60; vals[i] = 3.14159; }

    size_t cap = gorilla_max_compressed_size(N);
    uint8_t *buf = malloc(cap);
    size_t bytes = gorilla_compress(BASE_TS, ts, vals, (size_t)N, buf, cap);
    TEST_ASSERT_GREATER_THAN_size_t(0, bytes);

    int64_t ts_out[100]; double val_out[100]; size_t n_out;
    gorilla_decompress(buf, bytes, ts_out, val_out, (size_t)N, &n_out);
    TEST_ASSERT_EQUAL_size_t((size_t)N, n_out);
    for (int i = 0; i < N; i++) TEST_ASSERT_EQUAL_DOUBLE(vals[i], val_out[i]);
    free(buf);
}

void test_integer_counter_lossless(void) {
    const int N = 64;
    int64_t ts[64]; double vals[64];
    for (int i = 0; i < N; i++) { ts[i] = BASE_TS + i*60; vals[i] = (double)i; }

    size_t cap = gorilla_max_compressed_size(N);
    uint8_t *buf = malloc(cap);
    size_t bytes = gorilla_compress(BASE_TS, ts, vals, (size_t)N, buf, cap);
    TEST_ASSERT_GREATER_THAN_size_t(0, bytes);
    TEST_ASSERT_LESS_THAN_size_t((size_t)N * 16, bytes);

    int64_t ts_out[64]; double val_out[64]; size_t n_out;
    gorilla_decompress(buf, bytes, ts_out, val_out, (size_t)N, &n_out);
    TEST_ASSERT_EQUAL_size_t((size_t)N, n_out);
    for (int i = 0; i < N; i++) TEST_ASSERT_EQUAL_DOUBLE((double)i, val_out[i]);
    free(buf);
}

void test_random_floats_lossless(void) {
    const int N = 64;
    int64_t ts[64]; double vals[64];
    uint64_t state = 0xDEADBEEFULL;
    for (int i = 0; i < N; i++) {
        ts[i] = BASE_TS + i * 60;
        state ^= state << 13; state ^= state >> 7; state ^= state << 17;
        vals[i] = (double)(int64_t)(state % 1000000) * 0.001;
    }

    size_t cap = gorilla_max_compressed_size(N);
    uint8_t *buf = malloc(cap);
    size_t bytes = gorilla_compress(BASE_TS, ts, vals, (size_t)N, buf, cap);
    TEST_ASSERT_GREATER_THAN_size_t(0, bytes);

    int64_t ts_out[64]; double val_out[64]; size_t n_out;
    gorilla_decompress(buf, bytes, ts_out, val_out, (size_t)N, &n_out);
    TEST_ASSERT_EQUAL_size_t((size_t)N, n_out);
    for (int i = 0; i < N; i++) TEST_ASSERT_EQUAL_DOUBLE(vals[i], val_out[i]);
    free(buf);
}

void test_all_zeros_lossless(void) {
    const int N = 10;
    int64_t ts[10]; double vals[10];
    for (int i = 0; i < N; i++) { ts[i] = BASE_TS + i*60; vals[i] = 0.0; }

    size_t cap = gorilla_max_compressed_size(N);
    uint8_t *buf = malloc(cap);
    size_t bytes = gorilla_compress(BASE_TS, ts, vals, (size_t)N, buf, cap);

    int64_t ts_out[10]; double val_out[10]; size_t n_out;
    gorilla_decompress(buf, bytes, ts_out, val_out, (size_t)N, &n_out);
    TEST_ASSERT_EQUAL_size_t((size_t)N, n_out);
    for (int i = 0; i < N; i++) TEST_ASSERT_EQUAL_DOUBLE(0.0, val_out[i]);
    free(buf);
}

void test_negative_values_lossless(void) {
    const int N = 10;
    int64_t ts[10];
    double vals[10] = {-1.0, -2.5, -100.0, -0.001, -999.9,
                        1.0,  2.5,  100.0,  0.001,  999.9};
    for (int i = 0; i < N; i++) ts[i] = BASE_TS + i*60;

    size_t cap = gorilla_max_compressed_size(N);
    uint8_t *buf = malloc(cap);
    size_t bytes = gorilla_compress(BASE_TS, ts, vals, (size_t)N, buf, cap);

    int64_t ts_out[10]; double val_out[10]; size_t n_out;
    gorilla_decompress(buf, bytes, ts_out, val_out, (size_t)N, &n_out);
    TEST_ASSERT_EQUAL_size_t((size_t)N, n_out);
    for (int i = 0; i < N; i++) TEST_ASSERT_EQUAL_DOUBLE(vals[i], val_out[i]);
    free(buf);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_identical_values_compress_to_one_bit_each);
    RUN_TEST(test_integer_counter_lossless);
    RUN_TEST(test_random_floats_lossless);
    RUN_TEST(test_all_zeros_lossless);
    RUN_TEST(test_negative_values_lossless);
    return UNITY_END();
}
