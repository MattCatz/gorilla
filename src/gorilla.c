/*
 * gorilla.c - Gorilla time series compression implementation
 *
 * All section references are to:
 *   Pelkonen et al., "Gorilla: A Fast, Scalable, In-Memory Time Series
 *   Database", VLDB 2015. https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
 */

#include "gorilla.h"

#include <string.h>
#include <assert.h>

/* =========================================================================
 * Internal helpers
 * ========================================================================= */

/* Count leading zeros in a 64-bit word. Returns 64 for input 0. */
static inline int clz64(uint64_t x) {
    if (x == 0) return 64;
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_clzll((unsigned long long)x);
#else
    int n = 0;
    if ((x & 0xFFFFFFFF00000000ULL) == 0) { n += 32; x <<= 32; }
    if ((x & 0xFFFF000000000000ULL) == 0) { n += 16; x <<= 16; }
    if ((x & 0xFF00000000000000ULL) == 0) { n +=  8; x <<=  8; }
    if ((x & 0xF000000000000000ULL) == 0) { n +=  4; x <<=  4; }
    if ((x & 0xC000000000000000ULL) == 0) { n +=  2; x <<=  2; }
    if ((x & 0x8000000000000000ULL) == 0) { n +=  1; }
    return n;
#endif
}

/* Count trailing zeros in a 64-bit word. Returns 64 for input 0. */
static inline int ctz64(uint64_t x) {
    if (x == 0) return 64;
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_ctzll((unsigned long long)x);
#else
    int n = 0;
    if ((x & 0x00000000FFFFFFFFULL) == 0) { n += 32; x >>= 32; }
    if ((x & 0x000000000000FFFFULL) == 0) { n += 16; x >>= 16; }
    if ((x & 0x00000000000000FFULL) == 0) { n +=  8; x >>=  8; }
    if ((x & 0x000000000000000FULL) == 0) { n +=  4; x >>=  4; }
    if ((x & 0x0000000000000003ULL) == 0) { n +=  2; x >>=  2; }
    if ((x & 0x0000000000000001ULL) == 0) { n +=  1; }
    return n;
#endif
}

/* Reinterpret double <-> uint64 without UB */
static inline uint64_t dbl_to_u64(double d) {
    uint64_t u;
    memcpy(&u, &d, 8);
    return u;
}
static inline double u64_to_dbl(uint64_t u) {
    double d;
    memcpy(&d, &u, 8);
    return d;
}


/* =========================================================================
 * Bitstream writer
 * ========================================================================= */

int gorilla_bsw_init(gorilla_bsw_t *bs, uint8_t *buf, size_t cap) {
    if (!bs || !buf || cap == 0) return -1;
    bs->buf     = buf;
    bs->cap     = cap;
    bs->bit_len = 0;
    memset(buf, 0, cap);
    return 0;
}

int gorilla_bsw_write(gorilla_bsw_t *bs, uint64_t val, int nbits) {
    assert(nbits >= 1 && nbits <= 64);

    if (bs->bit_len + (size_t)nbits > bs->cap * 8)
        return -1;  /* overflow */

    /* Mask to nbits */
    if (nbits < 64)
        val &= ((uint64_t)1 << nbits) - 1;

    /* Write bit by bit, MSB first.
     * This is intentionally simple/correct; a production implementation
     * would batch whole bytes. */
    for (int i = nbits - 1; i >= 0; i--) {
        size_t byte_idx = bs->bit_len / 8;
        int    bit_idx  = 7 - (int)(bs->bit_len % 8);   /* MSB-first within byte */
        if ((val >> i) & 1)
            bs->buf[byte_idx] |= (uint8_t)(1u << bit_idx);
        bs->bit_len++;
    }
    return 0;
}

size_t gorilla_bsw_finish(gorilla_bsw_t *bs) {
    return (bs->bit_len + 7) / 8;
}


/* =========================================================================
 * Bitstream reader
 * ========================================================================= */

int gorilla_bsr_init(gorilla_bsr_t *bs, const uint8_t *buf, size_t bit_len) {
    if (!bs || !buf) return -1;
    bs->buf     = buf;
    bs->bit_len = bit_len;
    bs->bit_pos = 0;
    return 0;
}

int gorilla_bsr_read(gorilla_bsr_t *bs, int nbits, uint64_t *out) {
    assert(nbits >= 1 && nbits <= 64);

    if (bs->bit_pos + (size_t)nbits > bs->bit_len)
        return -1;  /* not enough bits */

    uint64_t result = 0;
    for (int i = 0; i < nbits; i++) {
        size_t byte_idx = bs->bit_pos / 8;
        int    bit_idx  = 7 - (int)(bs->bit_pos % 8);
        result = (result << 1) | ((bs->buf[byte_idx] >> bit_idx) & 1);
        bs->bit_pos++;
    }
    *out = result;
    return 0;
}


/* =========================================================================
 * Timestamp encoding  (paper §4.1.1)
 *
 * Control bits and ranges (Table in §4.1.1):
 *
 *   '0'          D == 0                  (1 bit total)
 *   '10'  + 7b   D in [-63,  64]         (9 bits total)
 *   '110' + 9b   D in [-255, 256]        (12 bits total)
 *   '1110'+ 12b  D in [-2047,2048]       (16 bits total)
 *   '1111'+ 32b  everything else         (36 bits total)
 *
 * Values are stored as sign-magnitude so that the bit width is fixed per
 * bucket. The paper says "store '10' followed by the value (7 bits)"; the
 * sign bit is included in those 7 bits (two's complement in the allocated
 * width).
 * ========================================================================= */

/* Encode a delta-of-delta value into the write stream. */
static int enc_timestamp_dod(gorilla_bsw_t *bs, int64_t dod) {
    if (dod == 0) {
        return gorilla_bsw_write(bs, 0, 1);           /* '0' */
    }
    if (dod >= -63 && dod <= 64) {
        if (gorilla_bsw_write(bs, 0x2, 2) < 0) return -1;   /* '10' */
        return gorilla_bsw_write(bs, (uint64_t)(dod & 0x7F), 7);
    }
    if (dod >= -255 && dod <= 256) {
        if (gorilla_bsw_write(bs, 0x6, 3) < 0) return -1;   /* '110' */
        return gorilla_bsw_write(bs, (uint64_t)(dod & 0x1FF), 9);
    }
    if (dod >= -2047 && dod <= 2048) {
        if (gorilla_bsw_write(bs, 0xE, 4) < 0) return -1;   /* '1110' */
        return gorilla_bsw_write(bs, (uint64_t)(dod & 0xFFF), 12);
    }
    /* Fallback: '1111' + 32-bit value */
    if (gorilla_bsw_write(bs, 0xF, 4) < 0) return -1;       /* '1111' */
    return gorilla_bsw_write(bs, (uint64_t)(dod & 0xFFFFFFFF), 32);
}

/* Decode a delta-of-delta value from the read stream. */
static int dec_timestamp_dod(gorilla_bsr_t *bs, int64_t *dod) {
    uint64_t bit;
    if (gorilla_bsr_read(bs, 1, &bit) < 0) return -1;

    if (bit == 0) {
        *dod = 0;
        return 0;
    }
    /* Read second control bit */
    if (gorilla_bsr_read(bs, 1, &bit) < 0) return -1;
    if (bit == 0) {
        /* '10': 7-bit value */
        uint64_t v;
        if (gorilla_bsr_read(bs, 7, &v) < 0) return -1;
        /* Sign-extend from 7 bits */
        *dod = (v & 0x40) ? (int64_t)(v | ~(uint64_t)0x7F) : (int64_t)v;
        return 0;
    }
    /* Read third control bit */
    if (gorilla_bsr_read(bs, 1, &bit) < 0) return -1;
    if (bit == 0) {
        /* '110': 9-bit value */
        uint64_t v;
        if (gorilla_bsr_read(bs, 9, &v) < 0) return -1;
        *dod = (v & 0x100) ? (int64_t)(v | ~(uint64_t)0x1FF) : (int64_t)v;
        return 0;
    }
    /* Read fourth control bit */
    if (gorilla_bsr_read(bs, 1, &bit) < 0) return -1;
    if (bit == 0) {
        /* '1110': 12-bit value */
        uint64_t v;
        if (gorilla_bsr_read(bs, 12, &v) < 0) return -1;
        *dod = (v & 0x800) ? (int64_t)(v | ~(uint64_t)0xFFF) : (int64_t)v;
        return 0;
    }
    /* '1111': 32-bit value */
    {
        uint64_t v;
        if (gorilla_bsr_read(bs, 32, &v) < 0) return -1;
        /* Sign-extend from 32 bits */
        *dod = (v & 0x80000000ULL)
             ? (int64_t)(v | ~(uint64_t)0xFFFFFFFF)
             : (int64_t)v;
        return 0;
    }
}


/* =========================================================================
 * Value encoding  (paper §4.1.2)
 *
 * Algorithm:
 *   §1: first value stored uncompressed (64 bits) — handled in enc_append
 *   §2: XOR with previous == 0 → write single '0' bit
 *   §3: XOR != 0:
 *       write '1', then either:
 *       (a) control bit '0': meaningful bits fall within previous block
 *           → just write the meaningful XOR bits (no leading/trailing counts)
 *       (b) control bit '1': write 5-bit leading-zero count,
 *                            6-bit meaningful-length count,
 *                            then the meaningful bits
 * ========================================================================= */

static int enc_value_xor(gorilla_bsw_t *bs,
                         uint64_t xor_val,
                         int     *leading_prev,
                         int     *trailing_prev,
                         uint64_t *xor_prev_out) {
    *xor_prev_out = xor_val;

    if (xor_val == 0) {
        return gorilla_bsw_write(bs, 0, 1);   /* '0' */
    }

    int leading  = clz64(xor_val);
    int trailing = ctz64(xor_val);
    int meaningful = 64 - leading - trailing;

    /* '1' control bit — non-zero XOR */
    if (gorilla_bsw_write(bs, 1, 1) < 0) return -1;

    /*
     * Case (a): reuse previous window.
     * "The block of meaningful bits falls within the block of previous
     *  meaningful bits, i.e., there are at least as many leading zeros
     *  and as many trailing zeros as with the previous value."
     * In this case we write '0' and just store the meaningful bits.
     */
    if (leading >= *leading_prev && trailing >= *trailing_prev) {
        if (gorilla_bsw_write(bs, 0, 1) < 0) return -1;   /* control '0' */
        int prev_meaningful = 64 - *leading_prev - *trailing_prev;
        /* Shift xor_val right to align with previous window.
         * Guard against UB: (uint64_t)1 << 64 is undefined. */
        uint64_t mask = (prev_meaningful == 64)
                      ? ~(uint64_t)0
                      : (((uint64_t)1 << prev_meaningful) - 1);
        uint64_t bits = (xor_val >> *trailing_prev) & mask;
        return gorilla_bsw_write(bs, bits, prev_meaningful);
    }

    /* Case (b): new window */
    if (gorilla_bsw_write(bs, 1, 1) < 0) return -1;       /* control '1' */
    /* 5 bits: number of leading zeros */
    if (gorilla_bsw_write(bs, (uint64_t)leading, 5) < 0) return -1;
    /* 6 bits: length of meaningful XOR value */
    if (gorilla_bsw_write(bs, (uint64_t)meaningful, 6) < 0) return -1;
    /* meaningful bits themselves */
    uint64_t bits = (xor_val >> trailing) & (((uint64_t)1 << meaningful) - 1);
    if (gorilla_bsw_write(bs, bits, meaningful) < 0) return -1;

    *leading_prev  = leading;
    *trailing_prev = trailing;
    return 0;
}

static int dec_value_xor(gorilla_bsr_t *bs,
                         uint64_t *val_prev,
                         uint64_t *xor_prev,
                         int      *leading_prev,
                         int      *trailing_prev,
                         double   *val_out) {
    uint64_t ctrl;
    if (gorilla_bsr_read(bs, 1, &ctrl) < 0) return -1;

    if (ctrl == 0) {
        /* Same as previous value */
        *val_out = u64_to_dbl(*val_prev);
        return 0;
    }

    /* Non-zero XOR: read inner control bit */
    if (gorilla_bsr_read(bs, 1, &ctrl) < 0) return -1;

    uint64_t xor_val;
    if (ctrl == 0) {
        /* Case (a): reuse previous window */
        int meaningful = 64 - *leading_prev - *trailing_prev;
        uint64_t bits;
        if (gorilla_bsr_read(bs, meaningful, &bits) < 0) return -1;
        xor_val = bits << *trailing_prev;
    } else {
        /* Case (b): new window */
        uint64_t lead_u, mlen_u;
        if (gorilla_bsr_read(bs, 5, &lead_u) < 0) return -1;
        if (gorilla_bsr_read(bs, 6, &mlen_u) < 0) return -1;
        int leading   = (int)lead_u;
        int meaningful = (int)mlen_u;
        int trailing  = 64 - leading - meaningful;
        uint64_t bits;
        if (gorilla_bsr_read(bs, meaningful, &bits) < 0) return -1;
        xor_val = bits << trailing;
        *leading_prev  = leading;
        *trailing_prev = trailing;
    }

    *xor_prev  = xor_val;
    *val_prev ^= xor_val;
    *val_out   = u64_to_dbl(*val_prev);
    return 0;
}


/* =========================================================================
 * Encoder public API
 * ========================================================================= */

int gorilla_enc_init(gorilla_enc_t *enc,
                     int64_t t_aligned,
                     uint8_t *buf, size_t cap) {
    if (!enc || !buf || cap == 0) return -1;
    memset(enc, 0, sizeof(*enc));

    if (gorilla_bsw_init(&enc->bs, buf, cap) < 0) return -1;

    enc->t_minus1  = t_aligned;
    enc->first_ts  = 1;
    enc->first_val = 1;

    /* Write the 64-bit aligned header timestamp (paper §4.1.1 §1) */
    if (gorilla_bsw_write(&enc->bs, (uint64_t)t_aligned, 64) < 0) return -1;
    /* Reserve 32 bits for point count; patched in gorilla_enc_finish(). */
    if (gorilla_bsw_write(&enc->bs, 0, 32) < 0) return -1;
    return 0;
}

int gorilla_enc_append(gorilla_enc_t *enc, int64_t ts, double value) {
    uint64_t u = dbl_to_u64(value);

    if (enc->first_ts) {
        /*
         * First timestamp: store as 14-bit delta from t_minus1 (§4.1.1 §1).
         * "14 bits is enough to span a bit more than 4 hours (16,384 seconds)."
         */
        int64_t delta = ts - enc->t_minus1;
        if (delta < 0 || delta > 16383) return -1;   /* out of 14-bit range */
        if (gorilla_bsw_write(&enc->bs, (uint64_t)delta, 14) < 0) return -1;

        enc->delta_prev = delta;
        enc->t_prev     = ts;
        enc->first_ts   = 0;
    } else {
        int64_t delta = ts - enc->t_prev;
        int64_t dod   = delta - enc->delta_prev;

        if (enc_timestamp_dod(&enc->bs, dod) < 0) return -1;

        enc->delta_prev = delta;
        enc->t_prev     = ts;
    }

    if (enc->first_val) {
        /* First value: stored uncompressed (§4.1.2 §1) */
        if (gorilla_bsw_write(&enc->bs, u, 64) < 0) return -1;
        enc->val_prev      = u;
        enc->xor_prev      = 0;
        enc->leading_prev  = 0;
        enc->trailing_prev = 0;
        enc->first_val     = 0;
    } else {
        uint64_t xor_val = enc->val_prev ^ u;
        if (enc_value_xor(&enc->bs, xor_val,
                          &enc->leading_prev, &enc->trailing_prev,
                          &enc->xor_prev) < 0) return -1;
        enc->val_prev = u;
    }

    enc->n_points++;
    return 0;
}

size_t gorilla_enc_finish(gorilla_enc_t *enc) {
    /* Patch the 32-bit point count into bits [64..95] of the buffer */
    size_t saved_bit_len = enc->bs.bit_len;
    enc->bs.bit_len = 64;  /* seek back to count field */
    gorilla_bsw_write(&enc->bs, (uint64_t)enc->n_points, 32);
    enc->bs.bit_len = saved_bit_len;
    return gorilla_bsw_finish(&enc->bs);
}


/* =========================================================================
 * Decoder public API
 * ========================================================================= */

int gorilla_dec_init(gorilla_dec_t *dec, const uint8_t *buf, size_t byte_len) {
    if (!dec || !buf || byte_len == 0) return -1;
    memset(dec, 0, sizeof(*dec));

    if (gorilla_bsr_init(&dec->bs, buf, byte_len * 8) < 0) return -1;

    /* Read the 64-bit header timestamp */
    uint64_t hdr;
    if (gorilla_bsr_read(&dec->bs, 64, &hdr) < 0) return -1;
    dec->t_minus1  = (int64_t)hdr;

    /* Read 32-bit point count */
    uint64_t npts;
    if (gorilla_bsr_read(&dec->bs, 32, &npts) < 0) return -1;
    dec->n_points = (uint32_t)npts;

    dec->first_ts  = 1;
    dec->first_val = 1;
    dec->points_read = 0;

    return 0;
}

int gorilla_dec_next(gorilla_dec_t *dec, int64_t *ts_out, double *val_out) {
    if (dec->points_read >= dec->n_points)
        return 0;   /* end of stream */

    /* ---- timestamp ---- */
    if (dec->first_ts) {
        uint64_t delta;
        if (gorilla_bsr_read(&dec->bs, 14, &delta) < 0) return -1;
        dec->t_prev     = dec->t_minus1 + (int64_t)delta;
        dec->delta_prev = (int64_t)delta;
        dec->first_ts   = 0;
    } else {
        int64_t dod;
        if (dec_timestamp_dod(&dec->bs, &dod) < 0) return -1;

        int64_t delta   = dec->delta_prev + dod;
        dec->t_prev    += delta;
        dec->delta_prev = delta;
    }
    *ts_out = dec->t_prev;

    /* ---- value ---- */
    if (dec->first_val) {
        uint64_t u;
        if (gorilla_bsr_read(&dec->bs, 64, &u) < 0) return -1;
        dec->val_prev      = u;
        dec->xor_prev      = 0;
        dec->leading_prev  = 0;
        dec->trailing_prev = 0;
        dec->first_val     = 0;
        *val_out = u64_to_dbl(u);
    } else {
        if (dec_value_xor(&dec->bs,
                          &dec->val_prev,
                          &dec->xor_prev,
                          &dec->leading_prev,
                          &dec->trailing_prev,
                          val_out) < 0) return -1;
    }

    dec->points_read++;
    return 1;
}


/* =========================================================================
 * Convenience wrappers
 * ========================================================================= */

size_t gorilla_compress(int64_t        t_aligned,
                        const int64_t *timestamps,
                        const double  *values,
                        size_t         n,
                        uint8_t       *out_buf,
                        size_t         out_cap) {
    gorilla_enc_t enc;
    if (gorilla_enc_init(&enc, t_aligned, out_buf, out_cap) < 0) return 0;
    for (size_t i = 0; i < n; i++) {
        if (gorilla_enc_append(&enc, timestamps[i], values[i]) < 0) return 0;
    }
    return gorilla_enc_finish(&enc);
}

int gorilla_decompress(const uint8_t *buf,
                       size_t         byte_len,
                       int64_t       *timestamps,
                       double        *values,
                       size_t         max_points,
                       size_t        *n_out) {
    gorilla_dec_t dec;
    if (gorilla_dec_init(&dec, buf, byte_len) < 0) return -1;

    size_t count = 0;
    int rc;
    while (count < max_points) {
        rc = gorilla_dec_next(&dec, &timestamps[count], &values[count]);
        if (rc == 0) break;    /* end of stream */
        if (rc < 0) return -1; /* error */
        count++;
    }
    *n_out = count;
    return 0;
}
