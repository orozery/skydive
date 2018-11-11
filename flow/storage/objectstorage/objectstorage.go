package objectstorage

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/google/gopacket/layers"

	"github.com/skydive-project/skydive/common"
	"github.com/skydive-project/skydive/config"
	"github.com/skydive-project/skydive/filters"
	"github.com/skydive-project/skydive/flow"
	"github.com/skydive-project/skydive/logging"
	"github.com/skydive-project/skydive/storage/objectstorage"
)

// stream is a series of consecutive persisted objects
type stream struct {
	// ID holds the time when the stream was created
	ID time.Time
	// SeqNumber holds the next available sequence number of this stream
	SeqNumber int
}

// ObjectStorage describes an object storage client
type ObjectStorage struct {
	bucket                     string
	objectPrefix               string
	currentStream              stream
	maxStreamDuration          time.Duration
	client                     objectstorage.Client
}

// StoreFlows writes a set of flows to the object storage service
func (c *ObjectStorage) StoreFlows(flows []*flow.Flow) error {
	if len(flows) == 0 {
		return nil
	}

	flowsString, err := json.Marshal(flows)
	if err != nil {
		logging.GetLogger().Error("Error encoding flows: ", err)
		return err
	}

	var firstTime int64 = math.MaxInt64
	var lastTime int64
	for _, fl := range flows {
		if fl.Last < firstTime {
			firstTime = fl.Last
		}
		if fl.Last > lastTime {
			lastTime = fl.Last
		}
	}
	metadata := map[string]*string{
		"first-timestamp": aws.String(strconv.FormatInt(firstTime, 10)),
		"last-timestamp":  aws.String(strconv.FormatInt(lastTime, 10)),
		"num-records":     aws.String(strconv.Itoa(len(flows))),
	}

	// gzip
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write([]byte(flowsString))
	w.Close()

	currentStream := c.currentStream
	if time.Since(currentStream.ID) >= c.maxStreamDuration {
		currentStream = stream{ID: time.Now()}
	}

	objectKey := strings.Join([]string{c.objectPrefix, currentStream.ID.UTC().Format("20060102T150405Z"), fmt.Sprintf("%08d.gz", currentStream.SeqNumber)}, "/")
	err = c.client.WriteObject(c.bucket, objectKey, string(b.Bytes()), "application/json", "gzip", metadata)

	if err != nil {
		logging.GetLogger().Error("Failed to write object: ", err)
		return err
	}

	currentStream.SeqNumber++
	c.currentStream = currentStream

	return nil
}

// SearchRawPackets searches flow raw packets matching filters in the database
func (c *ObjectStorage) SearchRawPackets(fsq filters.SearchQuery, packetFilter *filters.Filter) (map[string]*flow.RawPackets, error) {
	flowset, err := c.SearchFlows(fsq)
	if err != nil {
		return nil, err
	}
	flowset = flowset.Filter(packetFilter)
	rawpackets := make(map[string]*flow.RawPackets)
	for _, currFlow := range flowset.Flows {
		if fr, ok := rawpackets[currFlow.UUID]; ok {
			fr.RawPackets = append(fr.RawPackets, currFlow.LastRawPackets...)
		} else {
			linkType, err := currFlow.LinkType()
			if err != nil {
				linkType = layers.LinkTypeNull
			}
			rawpackets[currFlow.UUID] = &flow.RawPackets{
				LinkType:   linkType,
				RawPackets: currFlow.LastRawPackets,
			}
		}
	}
	return rawpackets, nil
}

// SearchMetrics searches flow metrics matching filters in the database
func (c *ObjectStorage) SearchMetrics(fsq filters.SearchQuery, metricFilter *filters.Filter) (map[string][]common.Metric, error) {
	flowset, err := c.SearchFlows(fsq)
	if err != nil {
		return nil, err
	}
	flowset = flowset.Filter(metricFilter)
	metrics := make(map[string][]common.Metric)
	for _, currFlow := range flowset.Flows {
		metrics[currFlow.UUID] = append(metrics[currFlow.UUID], currFlow.Metric)
	}
	return metrics, nil
}

func (c *ObjectStorage) getAllFlows() (*flow.FlowSet, error) {
	objectKeys, err := c.client.ListObjects(c.bucket, c.objectPrefix)
	if err != nil {
		logging.GetLogger().Error("Failed to list objects: ", err)
		return nil, err
	}

	flowset := flow.NewFlowSet()
	for _, objectKey := range objectKeys {
		objectBytes, err := c.client.ReadObject(c.bucket, *objectKey)
		if err != nil {
			logging.GetLogger().Error("Failed to read object: ", err)
			return nil, err
		}

		var flows []*flow.Flow
		err = json.Unmarshal(objectBytes, &flows)
		if err != nil {
			logging.GetLogger().Warning("Failed to JSON-decode object: ", err)
		} else {
			flowset.Flows = append(flowset.Flows, flows...)
		}
	}

	return flowset, nil
}

// SearchFlows search flow matching filters in the database
func (c *ObjectStorage) SearchFlows(fsq filters.SearchQuery) (*flow.FlowSet, error) {
	flowset, err := c.getAllFlows()
	if err != nil {
		return nil, err
	}

	return flowset.SearchQuery(fsq), nil
}

// Start the Object Storage client
func (c *ObjectStorage) Start() {
}

// Stop the Object Storage client
func (c *ObjectStorage) Stop() {
}

// New creates a new object storage backend client
func New(backend string) *ObjectStorage {
	path := "storage." + backend

	endpoint := config.GetConfig().GetString(path + ".endpoint")
	region := config.GetConfig().GetString(path + ".region")
	bucket := config.GetConfig().GetString(path + ".bucket")
	accessKey := config.GetConfig().GetString(path + ".access_key")
	secretKey := config.GetConfig().GetString(path + ".secret_key")
	objectPrefix := config.GetConfig().GetString(path + ".object_prefix")
	maxSecondsPerStream := config.GetConfig().GetInt(path + ".max_seconds_per_stream")

	client := objectstorage.New(endpoint, region, accessKey, secretKey)
	os := &ObjectStorage{
		bucket:            bucket,
		objectPrefix:      objectPrefix,
		maxStreamDuration: time.Second * time.Duration(maxSecondsPerStream),
		client:            client,
	}

	return os
}
