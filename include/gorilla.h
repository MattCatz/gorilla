/*
 * gorilla.h - Gorilla time series compression
 *
 * Implements the compression algorithm described in:
 *   "Gorilla: A Fast, Scalable, In-Memory Time Series Database"
 *   Pelkonen et al., VLDB 2015
 *   https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
 *
 * Two independent streams are interleaved in the output block:
 *   - Timestamps: delta-of-delta encoded, variable-length bit-packed (§4.1.1)
 *   - Values:     XOR with previous, leading/trailing zero encoded (§4.1.2)
 */

#ifndef GORILLA_H
#define GORILLA_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -------------------------------------------------------------------------
 * Bitstream
 * Raw bit-level read/write over a byte buffer.
 * ------------------------------------------------------------------------- */

typedef struct {
    uint8_t *buf;       /* backing buffer                   */
    size_t   cap;       /* capacity in bytes                */
    size_t   bit_len;   /* number of bits written so far    */
} gorilla_bsw_t;       /* write stream                     */

typedef struct {
    const uint8_t *buf; /* backing buffer                   */
    size_t         bit_len;  /* total valid bits in buffer  */
    size_t         bit_pos;  /* current read position       */
} gorilla_bsr_t;       /* read stream                      */

/*
 * Initialise a write stream. buf must be pre-allocated to at least cap bytes.
 * Returns 0 on success, -1 if buf is NULL.
 */
int gorilla_bsw_init(gorilla_bsw_t *bs, uint8_t *buf, size_t cap);

/*
 * Write `nbits` low-order bits of `val` into the stream.
 * Returns 0 on success, -1 if the buffer would overflow.
 * nbits must be 1..64.
 */
int gorilla_bsw_write(gorilla_bsw_t *bs, uint64_t val, int nbits);

/*
 * Finalise: zero-pad the last partial byte.
 * Returns total number of bytes used (ceil(bit_len / 8)).
 */
size_t gorilla_bsw_finish(gorilla_bsw_t *bs);

/*
 * Initialise a read stream from a finished write stream's buffer.
 * bit_len is the value returned by gorilla_bsw_finish() * 8, OR the
 * original bit_len field from the write stream — caller's choice as long
 * as it matches what was written.
 */
int gorilla_bsr_init(gorilla_bsr_t *bs, const uint8_t *buf, size_t bit_len);

/*
 * Read `nbits` bits and return them zero-extended in *out.
 * Returns 0 on success, -1 if fewer than nbits bits remain.
 * nbits must be 1..64.
 */
int gorilla_bsr_read(gorilla_bsr_t *bs, int nbits, uint64_t *out);

/* Number of bits remaining in a read stream. */
static inline size_t gorilla_bsr_remaining(const gorilla_bsr_t *bs) {
    return bs->bit_len - bs->bit_pos;
}


/* -------------------------------------------------------------------------
 * Encoder
 *
 * Paper §4.1: "Gorilla compresses data points within a time series with no
 * additional compression used across time series."
 *
 * Block layout (paper §4.2):
 *   [header: aligned t_minus1 (64-bit)]
 *   [first timestamp delta from t_minus1 (14 bits)]
 *   [first value uncompressed (64 bits)]
 *   [subsequent (timestamp, value) pairs...]
 * ------------------------------------------------------------------------- */

typedef struct {
    gorilla_bsw_t bs;

    /* timestamp state */
    int64_t  t_minus1;      /* block-aligned header timestamp    */
    int64_t  t_prev;        /* previous timestamp                */
    int64_t  delta_prev;    /* previous delta (for DoD)          */
    int      first_ts;      /* flag: haven't written first ts yet */

    /* value state */
    uint64_t val_prev;      /* previous value (bit pattern)      */
    uint64_t xor_prev;      /* previous XOR result               */
    int      leading_prev;  /* leading zeros of previous XOR     */
    int      trailing_prev; /* trailing zeros of previous XOR    */
    int      first_val;     /* flag: haven't written first val yet */

    uint32_t n_points;      /* number of points appended so far  */
} gorilla_enc_t;

/*
 * Initialise encoder.
 *
 * t_aligned: block-aligned starting timestamp (e.g. truncated to 2h boundary).
 *            Written verbatim as the 64-bit block header (paper §4.1.1 note 1).
 * buf / cap: output byte buffer.
 *
 * Returns 0 on success, -1 on bad arguments.
 */
int gorilla_enc_init(gorilla_enc_t *enc,
                     int64_t t_aligned,
                     uint8_t *buf, size_t cap);

/*
 * Append one (timestamp, value) pair.
 *
 * First call: timestamp stored as 14-bit delta from t_aligned (§4.1.1 §1).
 *             value stored uncompressed as 64 bits (§4.1.2 §1).
 * Subsequent calls: delta-of-delta timestamp + XOR value encoding.
 *
 * Returns 0 on success, -1 on buffer overflow or timestamp going backwards.
 */
int gorilla_enc_append(gorilla_enc_t *enc, int64_t timestamp, double value);

/*
 * Finalise the block. Returns byte length of the compressed output.
 * After this call the encoder must not be used again.
 */
size_t gorilla_enc_finish(gorilla_enc_t *enc);


/* -------------------------------------------------------------------------
 * Decoder
 * ------------------------------------------------------------------------- */

typedef struct {
    gorilla_bsr_t bs;

    /* timestamp state */
    int64_t  t_minus1;      /* block header timestamp            */
    int64_t  t_prev;        /* previous reconstructed timestamp  */
    int64_t  delta_prev;    /* previous delta                    */
    int      first_ts;

    /* value state */
    uint64_t val_prev;
    uint64_t xor_prev;
    int      leading_prev;
    int      trailing_prev;
    int      first_val;

    uint32_t n_points;      /* total points in block (from header) */
    uint32_t points_read;   /* how many decoded so far             */
} gorilla_dec_t;

/*
 * Initialise decoder from a compressed block.
 * buf/byte_len: the buffer returned by gorilla_enc_finish().
 *
 * Returns 0 on success, -1 on error.
 */
int gorilla_dec_init(gorilla_dec_t *dec, const uint8_t *buf, size_t byte_len);

/*
 * Decode the next (timestamp, value) pair.
 *
 * Returns:
 *   1  - ok, *ts and *val populated
 *   0  - end of stream (no more data points)
 *  -1  - decoding error (corrupt data)
 */
int gorilla_dec_next(gorilla_dec_t *dec, int64_t *ts, double *val);


/* -------------------------------------------------------------------------
 * Convenience: encode/decode whole arrays in one call
 * ------------------------------------------------------------------------- */

/*
 * Compress n (timestamp, value) pairs into out_buf.
 * out_buf must be at least gorilla_max_compressed_size(n) bytes.
 * t_aligned is the block header timestamp.
 *
 * Returns byte length written, or 0 on error.
 */
size_t gorilla_compress(int64_t t_aligned,
                        const int64_t *timestamps,
                        const double  *values,
                        size_t         n,
                        uint8_t       *out_buf,
                        size_t         out_cap);

/*
 * Decompress a block into caller-supplied arrays.
 * *n_out receives the number of points decoded.
 * timestamps/values must each be at least max_points elements long.
 *
 * Returns 0 on success, -1 on error.
 */
int gorilla_decompress(const uint8_t *buf,
                       size_t         byte_len,
                       int64_t       *timestamps,
                       double        *values,
                       size_t         max_points,
                       size_t        *n_out);

/*
 * Upper bound on compressed byte size for n data points.
 * Worst case: 64-bit header + 14-bit first delta + n * (32+1 ts bits +
 *             64+13 value bits) rounded up, plus one spare byte.
 */
static inline size_t gorilla_max_compressed_size(size_t n) {
    /* 64 header + 64 first_val + 14 first_delta + n*(33 + 78) bits */
    size_t bits = 64 + 64 + 14 + n * (33 + 78);
    return (bits / 8) + 2;
}

#ifdef __cplusplus
}
#endif

#endif /* GORILLA_H */
