package fake

// Client is a mock Object Storage client
type Client struct {
	// WriteError holds the error that should be returned on WriteObject
	WriteError error
	// WriteCounter holds the number of times that WriteObject was called
	WriteCounter int
	// LastBucket holds the bucket argument used on the last call to WriteObject
	LastBucket string
	// LastObjectKey holds the objectKey argument used on the last call to WriteObject
	LastObjectKey string
	// LastData holds the data argument used on the last call to WriteObject
	LastData string
	// LastContentType holds the contentType argument used on the last call to WriteObject
	LastContentType string
	// LastContentEncoding holds the contentEncoding argument used on the last call to WriteObject
	LastContentEncoding string
	// LastMetadata holds the metadata argument used on the last call to WriteObject
	LastMetadata map[string]*string
}

// WriteObject stores a single object
func (c *Client) WriteObject(bucket, objectKey, data, contentType, contentEncoding string, metadata map[string]*string) error {
	c.WriteCounter++
	c.LastBucket = bucket
	c.LastObjectKey = objectKey
	c.LastData = data
	c.LastContentType = contentType
	c.LastContentEncoding = contentEncoding
	c.LastMetadata = metadata

	return c.WriteError
}
