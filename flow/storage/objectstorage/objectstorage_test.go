package objectstorage

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/skydive-project/skydive/flow"
	"github.com/skydive-project/skydive/storage/objectstorage/fake"
)

func generateFlowArray(count int) []*flow.Flow {
	flows := make([]*flow.Flow, count)
	for i := 0; i < count; i++ {
		fl := &flow.Flow{}
		flows[i] = fl
	}

	return flows
}

func newTestObjectStorage() (*ObjectStorage, *fake.Client) {
	fakeClient := &fake.Client{}
	ds := &ObjectStorage{
		maxStreamDuration:          time.Second * time.Duration(86400),
		bucket:                     "myBucket",
		objectPrefix:               "myPrefix",
		client:                     fakeClient,
	}

	return ds, fakeClient
}

func assertEqual(t *testing.T, expected, actual interface{}) {
	if expected != actual {
		msg := "Equal assertion failed"
		_, file, no, ok := runtime.Caller(1)
		if ok {
			msg += fmt.Sprintf(" on %s:%d", file, no)
		}
		t.Fatalf("%s: (expected: %v, actual: %v)", msg, expected, actual)
	}
}

func assertNotEqual(t *testing.T, notExpected, actual interface{}) {
	if notExpected == actual {
		msg := "NotEqual assertion failed"
		_, file, no, ok := runtime.Caller(1)
		if ok {
			msg += fmt.Sprintf(" on %s:%d", file, no)
		}
		t.Fatalf("%s: (notExpected: %v, actual: %v)", msg, notExpected, actual)
	}
}

func Test_StoreFlows_Positive(t *testing.T) {
	c, fakeClient := newTestObjectStorage()
	flows := generateFlowArray(10)
	for i := 0; i < 10; i++ {
		flows[i].Last = int64(i)
	}

	assertEqual(t, nil, c.StoreFlows(flows))
	assertEqual(t, 1, fakeClient.WriteCounter)
	assertEqual(t, 1, c.currentStream.SeqNumber)

	// gzip decompress
	r, err := gzip.NewReader(bytes.NewBuffer([]byte(fakeClient.LastData)))
	defer r.Close()
	if err != nil {
		t.Fatal("Error in gzip decompress: ", err)
	}
	uncompressedData, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal("Error in gzip reading: ", err)
	}

	var decodedFlows []*flow.Flow
	if err := json.Unmarshal(uncompressedData, &decodedFlows); err != nil {
		t.Fatal("JSON parsing failed: ", err)
	}

	assertEqual(t, len(flows), len(decodedFlows))
	assertEqual(t, "0", *fakeClient.LastMetadata["first-timestamp"])
	assertEqual(t, "9", *fakeClient.LastMetadata["last-timestamp"])
	assertEqual(t, strconv.Itoa(len(flows)), *fakeClient.LastMetadata["num-records"])

	assertEqual(t, "application/json", fakeClient.LastContentType)
	assertEqual(t, "gzip", fakeClient.LastContentEncoding)
}

func Test_StoreFlows_maxStreamDuration(t *testing.T) {
	c, _ := newTestObjectStorage()
	c.maxStreamDuration = time.Second

	c.StoreFlows(generateFlowArray(1))
	assertEqual(t, 1, c.currentStream.SeqNumber)
	streamID := c.currentStream.ID
	c.StoreFlows(generateFlowArray(1))
	assertEqual(t, 2, c.currentStream.SeqNumber)
	assertEqual(t, streamID, c.currentStream.ID)

	time.Sleep(time.Second)

	c.StoreFlows(generateFlowArray(1))
	assertEqual(t, 1, c.currentStream.SeqNumber)
	assertNotEqual(t, streamID, c.currentStream.ID)
}

func Test_StoreFlows_WriteObjectError(t *testing.T) {
	c, fakeClient := newTestObjectStorage()
	myError := errors.New("my error")
	fakeClient.WriteError = myError

	assertEqual(t, myError, c.StoreFlows(generateFlowArray(1)))
}
