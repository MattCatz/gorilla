/*
 * test_streaming.c - gorilla_enc_t / gorilla_dec_t streaming API
 */
#include "unity.h"
#include "gorilla.h"
#include <stdlib.h>
#include <string.h>

void setUp(void)    {}
void tearDown(void) {}

#define BASE_TS 1700000000LL

void test_streaming_encode_decode_roundtrip(void) {
    const int N = 20;
    size_t cap = gorilla_max_compressed_size(N);
    uint8_t *buf = malloc(cap);

    gorilla_enc_t enc;
    TEST_ASSERT_EQUAL_INT(0, gorilla_enc_init(&enc, BASE_TS, buf, cap));
    for (int i = 0; i < N; i++)
        TEST_ASSERT_EQUAL_INT(0,
            gorilla_enc_append(&enc, BASE_TS + i * 60, (double)(i * i)));
    size_t bytes = gorilla_enc_finish(&enc);
    TEST_ASSERT_GREATER_THAN_size_t(0, bytes);

    gorilla_dec_t dec;
    TEST_ASSERT_EQUAL_INT(0, gorilla_dec_init(&dec, buf, bytes));

    int64_t ts; double val;
    for (int i = 0; i < N; i++) {
        TEST_ASSERT_EQUAL_INT(1, gorilla_dec_next(&dec, &ts, &val));
        TEST_ASSERT_EQUAL_INT64(BASE_TS + i * 60, ts);
        TEST_ASSERT_EQUAL_DOUBLE((double)(i * i), val);
    }
    /* One more call should return 0 (end of stream), not -1 (error) */
    TEST_ASSERT_EQUAL_INT(0, gorilla_dec_next(&dec, &ts, &val));

    free(buf);
}

void test_streaming_end_of_stream_is_clean(void) {
    /* Verify that dec_next returns 0 (not -1) at end */
    int64_t ts  = BASE_TS + 60;
    double  val = 42.0;
    size_t  cap = gorilla_max_compressed_size(1);
    uint8_t *buf = malloc(cap);

    gorilla_enc_t enc;
    gorilla_enc_init(&enc, BASE_TS, buf, cap);
    gorilla_enc_append(&enc, ts, val);
    size_t bytes = gorilla_enc_finish(&enc);

    gorilla_dec_t dec;
    gorilla_dec_init(&dec, buf, bytes);

    int64_t ts_out; double val_out;
    TEST_ASSERT_EQUAL_INT(1, gorilla_dec_next(&dec, &ts_out, &val_out));
    TEST_ASSERT_EQUAL_INT(0, gorilla_dec_next(&dec, &ts_out, &val_out));
    TEST_ASSERT_EQUAL_INT(0, gorilla_dec_next(&dec, &ts_out, &val_out)); /* idempotent */

    free(buf);
}

void test_streaming_matches_convenience_api(void) {
    /* gorilla_compress / gorilla_decompress must produce identical results
     * to the streaming enc/dec API */
    const int N = 15;
    int64_t ts[15]; double vals[15];
    for (int i = 0; i < N; i++) {
        ts[i]   = BASE_TS + i * 60;
        vals[i] = (double)i * 1.5 - 5.0;
    }

    size_t cap = gorilla_max_compressed_size(N);
    uint8_t *buf_conv = malloc(cap);
    uint8_t *buf_stream = malloc(cap);

    /* Convenience API */
    size_t bytes_conv =
        gorilla_compress(BASE_TS, ts, vals, (size_t)N, buf_conv, cap);

    /* Streaming API */
    gorilla_enc_t enc;
    gorilla_enc_init(&enc, BASE_TS, buf_stream, cap);
    for (int i = 0; i < N; i++)
        gorilla_enc_append(&enc, ts[i], vals[i]);
    size_t bytes_stream = gorilla_enc_finish(&enc);

    TEST_ASSERT_EQUAL_size_t(bytes_conv, bytes_stream);
    TEST_ASSERT_EQUAL_MEMORY(buf_conv, buf_stream, bytes_conv);

    free(buf_conv);
    free(buf_stream);
}

void test_multiple_blocks_independent(void) {
    /* Each block is self-contained; two independent compressions of the same
     * data should produce the same bytes */
    const int N = 10;
    int64_t ts[10]; double vals[10];
    for (int i = 0; i < N; i++) { ts[i] = BASE_TS + i*60; vals[i] = (double)i; }

    size_t cap = gorilla_max_compressed_size(N);
    uint8_t *buf1 = malloc(cap);
    uint8_t *buf2 = malloc(cap);

    size_t b1 = gorilla_compress(BASE_TS, ts, vals, (size_t)N, buf1, cap);
    size_t b2 = gorilla_compress(BASE_TS, ts, vals, (size_t)N, buf2, cap);

    TEST_ASSERT_EQUAL_size_t(b1, b2);
    TEST_ASSERT_EQUAL_MEMORY(buf1, buf2, b1);

    free(buf1); free(buf2);
}

/* -------------------------------------------------------------------------
 * Runner
 * ------------------------------------------------------------------------- */
int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_streaming_encode_decode_roundtrip);
    RUN_TEST(test_streaming_end_of_stream_is_clean);
    RUN_TEST(test_streaming_matches_convenience_api);
    RUN_TEST(test_multiple_blocks_independent);
    return UNITY_END();
}
