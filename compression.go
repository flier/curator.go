package curator

type CompressionProvider interface {
	Compress(path string, data []byte) ([]byte, error)

	Decompress(path string, compressedData []byte) ([]byte, error)
}
