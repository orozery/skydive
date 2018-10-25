package objectstorage

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Client allows uploading objects to an object storage service
type Client interface {
	// WriteObject stores a single object
	WriteObject(bucket, objectKey, data, contentType, contentEncoding string, metadata map[string]*string) error
}

// S3Client allows uploading objects to an S3-compatible object storage service
type S3Client struct {
	s3Client *s3.S3
}

// WriteObject stores a single object
func (s *S3Client) WriteObject(bucket, objectKey, data, contentType, contentEncoding string, metadata map[string]*string) error {
	_, err := s.s3Client.PutObject(&s3.PutObjectInput{
		Body:            strings.NewReader(data),
		Bucket:          aws.String(bucket),
		ContentType:     aws.String(contentType),
		ContentEncoding: aws.String(contentEncoding),
		Key:             aws.String(objectKey),
		Metadata:        metadata,
	})

	return err
}

// New creates a new S3-compatible object storage client
func New(endpoint, region, accessKey, secretKey string) Client {
	s3Client := s3.New(session.New(&aws.Config{
		S3ForcePathStyle: aws.Bool(true),
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
		Region:           aws.String(region),
	}))
	return &S3Client{
		s3Client: s3Client,
	}
}
