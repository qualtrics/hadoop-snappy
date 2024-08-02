# Hadoop Snappy Reader
[![Go Reference](https://pkg.go.dev/badge/github.com/qualtrics/hadoop-snappy.svg)](https://pkg.go.dev/github.com/qualtrics/hadoop-snappy)
![Build Status](https://github.com/qualtrics/hadoop-snappy/actions/workflows/ci.yml/badge.svg)
![Coverage](https://raw.githubusercontent.com/qualtrics/hadoop-snappy/badges/.badges/main/coverage.svg)

Small library that provides a reader for reading Hadoop Snappy encoded data. See the [Go Package documentation](https://pkg.go.dev/github.com/qualtrics/hadoop-snappy) for more information and examples of how to use the reader.

There are not currently plans to implement a writer, as the main utility of this library is to read and use data already produced by the Hadoop ecosystem. However, we are open to extending this library to support a writer or other use cases if there is interest.

## Developing

### Prerequisites
1. [Install Go](https://go.dev/doc/install)

### Run Tests
```bash
go test ./...
```

### Creating Test Data
1. Install `snzip`
   - Mac: `brew install snzip`
   - Other: [Instructions](https://github.com/kubo/snzip?tab=readme-ov-file#installation)
1. Add the uncompressed file to `testdata/`
1. Create the compressed file with `snzip -t hadoop-snappy -k testfile/{uncompressed file}`

## Release
Be sure to understand how [Go Module publishing](https://go.dev/blog/publishing-go-modules) works, especially semantic versioning.

To release simply create a new semantically versioned tag and push it.
```bash
# Create a new semantic versioned tag with release notes
git tag -a v1.0.0 -m "release notes"

# Push the tag to the remote repository
git push origin v1.0.0
```

## Hadoop-Snappy Stream Encoding Format
The Hadoop format of snappy is similar to regular snappy block encoding,
except that instead of compressing into one big block, Hadoop will create
a stream of frames where each frame contains blocks that can each be
independently decoded. A frame can contain 1 or more blocks and a stream
can contain 1 or more frames.

Each FRAME begins with a 4 byte header, which represents the total length
of the frame after being DECOMPRESSED (i.e. once we're done decompressing
the frame, this is how long the decompressed frame will be). This 4 byte
header is a big endian encoded uint32. The header is not included in the
total length of the frame.

Each BLOCK in the frame also begins with a 4 byte header that is the
COMPRESSED length of the block (i.e. how many bytes we need to read from
the stream to get the entire block before we can decompress it). This
header is also a big endian encoded uint32. The header is not included in
the total length of the block.

The stream structure is as follows
```
'['   == start of stream
']'   == end of stream
'|'   == component separator (symbolic only as the actual data has no padding or separators)
'...' == abbreviated

[ frame 1 header | block 1 header | block 1 | block 2 header | block 2 | ... | frame 2 header | block 1 header | block 1 | ... ]
```

The format of each individual snappy block can be found [here](https://github.com/google/snappy/blob/main/format_description.txt).