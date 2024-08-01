package snappy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"testing"

	gsnappy "github.com/golang/snappy"
)

var errTestingError = errors.New("testing error")

func TestSnappyReader(t *testing.T) {
	tests := map[string]struct {
		input          io.Reader
		expectedOutput []byte
		expectedErr    error
	}{
		"decompress hadoop compressed data": {
			input:          bytes.NewReader(mustReadFile("testdata/test.jsonl.snappy")),
			expectedOutput: mustReadFile("testdata/test.jsonl"),
		},
		"decompress large hadoop compressed data": {
			input:          bytes.NewReader(mustReadFile("testdata/shakespeare.txt.snappy")),
			expectedOutput: mustReadFile("testdata/shakespeare.txt"),
		},
		"decompress empty stream": {
			input:          bytes.NewReader([]byte{}),
			expectedOutput: []byte{},
		},
		"returns an error when decompressing truncated snappy data": {
			input:       bytes.NewReader(mustReadFile("testdata/truncated.snappy")),
			expectedErr: io.ErrUnexpectedEOF,
		},
		"returns an error when the frame size header is larger than the remaining stream": {
			input: func() io.Reader {
				data := mustReadFile("testdata/test.jsonl.snappy")
				// data[0] is most significant byte in the frame header, which is 0x00 (0) in the unaltered file
				// we should now be setting it to 0x01 (1)
				data[0] += 1
				return bytes.NewReader(data)
			}(),
			expectedErr: io.ErrUnexpectedEOF,
		},
		"returns an error when the frame size header is smaller than the actual frame size": {
			input: func() io.Reader {
				data := mustReadFile("testdata/test.jsonl.snappy")
				// data[3] is least significant byte in the frame header, which is 0x40 (64) in the unaltered file
				// we should now be setting it to 0x3F (63)
				data[3] -= 1
				return bytes.NewReader(data)
			}(),
			expectedErr: errDecompressedTooLarge,
		},
		"returns an error when the block size header is larger than the remaining stream": {
			input: func() io.Reader {
				data := mustReadFile("testdata/test.jsonl.snappy")
				// data[4] is most significant byte in the block header, which is 0x00 (0) in the unaltered file
				// we should now be setting it to 0x01 (1)
				data[4] += 1
				return bytes.NewReader(data)
			}(),
			expectedErr: io.ErrUnexpectedEOF,
		},
		"returns an error when the block size header is smaller than the actual block size": {
			input: func() io.Reader {
				data := mustReadFile("testdata/test.jsonl.snappy")
				// data[7] is least significant byte in the block header, which is 0x76 (118) in the unaltered file
				// we should now be setting it to 0x75 (117)
				data[7] += 1
				return bytes.NewReader(data)
			}(),
			expectedErr: io.ErrUnexpectedEOF,
		},
		"returns an error when it encounters a zero length block before reaching the end of the frame": {
			input: bytes.NewReader([]byte{
				// frame header of size 0xFF (255)
				0x00, 0x00, 0x00, 0xFF,
				// block header of size 0x00 (0)
				0x00, 0x00, 0x00, 0x00,
			}),
			expectedErr: errEmptyBlock,
		},
		"returns an error when it encounters a corrupted snappy preamble": {
			input: func() io.Reader {
				data := mustReadFile("testdata/test.jsonl.snappy")
				// data[8:9] are the snappy preamble for the test file's first block.
				// it is the varint little endian representation of the block's uncompressed
				// size. We will override the preamble making it too large for snappy to use.
				data[8] = 0xFF
				data[9] = 0xFF
				data[10] = 0xFF
				data[11] = 0xFF
				return bytes.NewReader(data)
			}(),
			expectedErr: gsnappy.ErrCorrupt,
		},
		"returns an error when it encounters a correctly sized but corrupted snappy block": {
			input: func() io.Reader {
				data := mustReadFile("testdata/test.jsonl.snappy")
				// Modify only the block of data, not the snappy preamble
				blockSize := int(binary.BigEndian.Uint32(data[4:8]))
				// we will skip the 2 bytes of the preamble
				blockSize -= 2

				// append non-snappy data after the snappy preamble
				data = append(data[:10], bytes.Repeat([]byte{0xFF}, blockSize)...)
				return bytes.NewReader(data)
			}(),
			expectedErr: gsnappy.ErrCorrupt,
		},
		"returns an error when the underlying reader returns an error": {
			input: &errorReader{
				err: errTestingError,
			},
			expectedErr: errTestingError,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			r := NewReader(test.input)
			output, err := io.ReadAll(r)
			if test.expectedErr == nil {
				if err != nil {
					t.Fatalf("unexpected error from snappy reader: %v", err)
				}

				if !bytes.Equal(test.expectedOutput, output) {
					// we don't try to print the difference because it is likely our output was garbage
					// and instead will let the developer debug it if encountered
					t.Errorf("output from snappy reader does not match expected")
				}
			} else {
				if !errors.Is(err, test.expectedErr) {
					t.Errorf("error from snappy reader does not match expected: got = (%v), want = (%v)", err, test.expectedErr)
				}
			}
		})
	}
}

func mustReadFile(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}

	return data
}

type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}
