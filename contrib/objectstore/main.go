package main

import (
	"fmt"
	"net/url"
	"os"

	"github.com/skydive-project/skydive/config"
	"github.com/skydive-project/skydive/contrib/objectstore/subscriber"
	shttp "github.com/skydive-project/skydive/http"
	"github.com/skydive-project/skydive/logging"
)

const defaultConfigurationFile = "/etc/skydive-objectstore.yml"

func main() {
	if err := config.InitConfig("file", []string{defaultConfigurationFile}); err != nil {
		logging.GetLogger().Error(fmt.Errorf("Failed to initialize config: %s", err.Error()))
		os.Exit(1)
	}

	if err := config.InitLogging(); err != nil {
		logging.GetLogger().Error(fmt.Errorf("Failed to initialize logging system: %s", err.Error()))
		os.Exit(1)
	}

	cfg := config.GetConfig()

	endpoint := cfg.GetString("endpoint")
	region := cfg.GetString("region")
	bucket := cfg.GetString("bucket")
	accessKey := cfg.GetString("access_key")
	secretKey := cfg.GetString("secret_key")
	objectPrefix := cfg.GetString("object_prefix")
	subscriberURLString := cfg.GetString("subscriber_url")
	subscriberUsername := cfg.GetString("subscriber_username")
	subscriberPassword := cfg.GetString("subscriber_password")
	maxSecondsPerStream := cfg.GetInt("max_seconds_per_stream")

	authOptions := &shttp.AuthenticationOpts{
		Username: subscriberUsername,
		Password: subscriberPassword,
	}

	subscriberURL, err := url.Parse(subscriberURLString)
	if err != nil {
		logging.GetLogger().Error(fmt.Errorf("Failed to parse subscriber URL: %s", err.Error()))
		os.Exit(1)
	}

	s := subscriber.New(endpoint, region, bucket, accessKey, secretKey, objectPrefix, maxSecondsPerStream, subscriberURL, authOptions)

	return s
}
