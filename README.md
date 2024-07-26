# Hadoop Snappy Reader
Small library that provides a reader for reading Hadoop Snappy encoded data. See the Go Package documentation for more information on the format and how to use the reader.

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
Be sure to understand how [Go Module publishing](https://go.dev/blog/publishing-go-modules) works, especially semantic versioning. To release simply create a new semantically versioned tag and push it.
```bash
# Create a new semantic versioned tag with release notes
git tag -a v1.0.0 -m "release notes"

# Push the tag to the remote repository
git push origin v1.0.0
```