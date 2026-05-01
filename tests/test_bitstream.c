/*
 * test_bitstream.c - Unit tests for gorilla_bsw_t / gorilla_bsr_t
 */
#include "unity.h"
#include "gorilla.h"

void setUp(void)    {}
void tearDown(void) {}

/* -------------------------------------------------------------------------
 * Individual tests
 * ------------------------------------------------------------------------- */

void test_three_bits_fit_in_one_byte(void) {
    uint8_t buf[8] = {0};
    gorilla_bsw_t bsw;
    TEST_ASSERT_EQUAL_INT(0, gorilla_bsw_init(&bsw, buf, sizeof(buf)));
    TEST_ASSERT_EQUAL_INT(0, gorilla_bsw_write(&bsw, 1, 1));
    TEST_ASSERT_EQUAL_INT(0, gorilla_bsw_write(&bsw, 0, 1));
    TEST_ASSERT_EQUAL_INT(0, gorilla_bsw_write(&bsw, 1, 1));
    TEST_ASSERT_EQUAL_size_t(1, gorilla_bsw_finish(&bsw));
}

void test_single_bit_roundtrip(void) {
    uint8_t buf[8] = {0};
    gorilla_bsw_t bsw;
    gorilla_bsr_t bsr;
    uint64_t out;

    gorilla_bsw_init(&bsw, buf, sizeof(buf));
    gorilla_bsw_write(&bsw, 1, 1);
    gorilla_bsw_write(&bsw, 0, 1);
    gorilla_bsw_write(&bsw, 1, 1);
    gorilla_bsw_finish(&bsw);

    gorilla_bsr_init(&bsr, buf, 3);
    gorilla_bsr_read(&bsr, 1, &out); TEST_ASSERT_EQUAL_UINT64(1, out);
    gorilla_bsr_read(&bsr, 1, &out); TEST_ASSERT_EQUAL_UINT64(0, out);
    gorilla_bsr_read(&bsr, 1, &out); TEST_ASSERT_EQUAL_UINT64(1, out);
}

void test_64bit_roundtrip(void) {
    uint8_t buf[16] = {0};
    gorilla_bsw_t bsw;
    gorilla_bsr_t bsr;
    uint64_t out;

    gorilla_bsw_init(&bsw, buf, sizeof(buf));
    gorilla_bsw_write(&bsw, 0xDEADBEEFCAFEBABEULL, 64);
    gorilla_bsw_finish(&bsw);

    gorilla_bsr_init(&bsr, buf, 64);
    gorilla_bsr_read(&bsr, 64, &out);
    TEST_ASSERT_EQUAL_UINT64(0xDEADBEEFCAFEBABEULL, out);
}

void test_multi_width_values(void) {
    uint8_t buf[8] = {0};
    gorilla_bsw_t bsw;
    gorilla_bsr_t bsr;
    uint64_t out;

    gorilla_bsw_init(&bsw, buf, sizeof(buf));
    gorilla_bsw_write(&bsw, 5,   3);   /* 101     */
    gorilla_bsw_write(&bsw, 7,   4);   /* 0111    */
    gorilla_bsw_write(&bsw, 255, 8);   /* 11111111*/
    gorilla_bsw_finish(&bsw);

    gorilla_bsr_init(&bsr, buf, 15);
    gorilla_bsr_read(&bsr, 3, &out); TEST_ASSERT_EQUAL_UINT64(5,   out);
    gorilla_bsr_read(&bsr, 4, &out); TEST_ASSERT_EQUAL_UINT64(7,   out);
    gorilla_bsr_read(&bsr, 8, &out); TEST_ASSERT_EQUAL_UINT64(255, out);
}

void test_read_past_end_returns_error(void) {
    uint8_t buf[1] = {0xFF};
    gorilla_bsr_t bsr;
    uint64_t out;

    gorilla_bsr_init(&bsr, buf, 4);
    TEST_ASSERT_EQUAL_INT(0, gorilla_bsr_read(&bsr, 4, &out));
    TEST_ASSERT_EQUAL_INT(-1, gorilla_bsr_read(&bsr, 1, &out));  /* exhausted */
}

void test_write_overflow_returns_error(void) {
    uint8_t buf[1] = {0};
    gorilla_bsw_t bsw;
    gorilla_bsw_init(&bsw, buf, 1);    /* only 8 bits */
    TEST_ASSERT_EQUAL_INT(0,  gorilla_bsw_write(&bsw, 0xFF, 8));
    TEST_ASSERT_EQUAL_INT(-1, gorilla_bsw_write(&bsw, 1, 1));    /* no room */
}

/* -------------------------------------------------------------------------
 * Runner
 * ------------------------------------------------------------------------- */
int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_three_bits_fit_in_one_byte);
    RUN_TEST(test_single_bit_roundtrip);
    RUN_TEST(test_64bit_roundtrip);
    RUN_TEST(test_multi_width_values);
    RUN_TEST(test_read_past_end_returns_error);
    RUN_TEST(test_write_overflow_returns_error);
    return UNITY_END();
}
