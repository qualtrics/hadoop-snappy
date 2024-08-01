/*
Package snappy implements decompression for the Hadoop format of snappy; a
compression scheme internal to the Hadoop ecosystem and HDFS.
*/
package snappy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"
)

const (
	// headerLength is the length in bytes of both the frame and block headers.
	headerLength = 4
)

var (
	errEmptyBlock           = errors.New("hadoop-snappy: zero length block in input stream")
	errDecompressedTooLarge = errors.New("decompressed frame larger than expected")
)

// Reader wraps a hadoop-snappy compressed data stream and decompresses
// the stream as it is read by the caller.
type Reader struct {
	// in is the compressed data stream we are reading from.
	in io.Reader
	// currentBlock is a block we have decompressed and are
	// currently reading from.
	currentBlock *bytes.Reader
	// decompressedBuf is used simply as a mechanism to resize
	// and reuse the underlying buffer as we are decompressing
	// blocks, to avoid reallocating a slice unnecessarily.
	decompressedBuf bytes.Buffer
	// compressedBuf is used to read the compressed data from
	// the input stream.
	compressedBuf bytes.Buffer
	// frameRemaining represents how many more uncompressed bytes
	// we have left in the current frame.
	frameRemaining int
}

// NewReader returns a Reader that can read the hadoop-snappy compressed format
// that will be read from the io.Reader in.
//
// Reading from an input stream that is not hadoop-snappy compressed will result
// in undefined behavior. Because there is no data signature to detect the
// compression format, the reader can only try to read the stream and will likely
// return an error, but it may return garbage data instead.
func NewReader(in io.Reader) *Reader {
	return &Reader{
		in:           in,
		currentBlock: bytes.NewReader([]byte{}),
	}
}

// Read implements the io.Reader interface. Read will return the
// decompressed data from the compressed input data stream. Read
// returns io.EOF when all data has been decompressed and read.
func (r *Reader) Read(out []byte) (int, error) {
	if r.currentBlock.Len() > 0 {
		return r.currentBlock.Read(out)
	}

	if r.frameRemaining == 0 {
		err := r.nextFrame()
		if err != nil {
			return 0, err
		}
	}

	err := r.nextBlock()
	if err != nil {
		return 0, err
	}

	return r.currentBlock.Read(out)
}

func (r *Reader) nextFrame() error {
	uncompressedSize, err := readHeader(r.in)
	if err != nil {
		if errors.Is(err, io.EOF) {
			// The start of a new frame is the one spot we expect to reach EOF.
			// To work correctly, Read must return EOF itself, not an error
			// wrapping EOF, because callers will test for EOF using =="
			return io.EOF
		}

		return fmt.Errorf("hadoop-snappy: read frame header: %w", err)
	}

	r.frameRemaining = uncompressedSize

	return nil
}

func (r *Reader) nextBlock() error {
	err := r.readNextBlock()
	if err != nil {
		return err
	}

	return r.decompress()
}

func (r *Reader) readNextBlock() error {
	nextBlockLength, err := readHeader(r.in)
	if err != nil {
		if errors.Is(err, io.EOF) && r.frameRemaining != 0 {
			err = io.ErrUnexpectedEOF
		}

		return fmt.Errorf("hadoop-snappy: read block header: %w", err)
	}

	if nextBlockLength == 0 && r.frameRemaining != 0 {
		return errEmptyBlock
	}

	r.compressedBuf.Reset()
	_, err = io.CopyN(&r.compressedBuf, r.in, int64(nextBlockLength))
	if err != nil {
		if errors.Is(err, io.EOF) {
			// We should not encounter EOF here because we should have at least
			// nextBlockLength left in the stream. If we encounter EOF here than
			// the stream is malformed.
			err = io.ErrUnexpectedEOF
		}

		return fmt.Errorf("hadoop-snappy: read block: %w", err)
	}

	return nil
}

func (r *Reader) decompress() error {
	decompressedLength, err := snappy.DecodedLen(r.compressedBuf.Bytes())
	if err != nil {
		return fmt.Errorf("hadoop-snappy: determine block decoded length: %w", err)
	}

	if decompressedLength > r.frameRemaining {
		return fmt.Errorf("hadoop-snappy: decompress block: %w", errDecompressedTooLarge)
	}

	r.frameRemaining -= decompressedLength

	r.decompressedBuf.Reset()
	r.decompressedBuf.Grow(decompressedLength)

	decompressed, err := snappy.Decode(r.decompressedBuf.Bytes(), r.compressedBuf.Bytes())
	if err != nil {
		return fmt.Errorf("hadoop-snappy: decompress block: %w", err)
	}

	r.currentBlock = bytes.NewReader(decompressed)

	return nil
}

func readHeader(r io.Reader) (int, error) {
	headerBuf := make([]byte, headerLength)
	_, err := io.ReadFull(r, headerBuf)
	if err != nil {
		return 0, err
	}

	return int(binary.BigEndian.Uint32(headerBuf)), nil
}
