package snappy

import (
	"bytes"
	"fmt"
	"io"
)

func Example() {
	// encodedData is the string "Hello, world!" encoded using the hadoop-snappy compression format.
	encodedData := []byte{0x00, 0x00, 0x00, 0x0D, 0x00, 0x00, 0x00, 0x0F, 0x0D, 0x30, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x77, 0x6F, 0x72, 0x6C, 0x64, 0x21}

	r := NewReader(bytes.NewReader(encodedData))

	output, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}

	// Output: Hello, world!
	fmt.Printf("%s\n", output)
}
