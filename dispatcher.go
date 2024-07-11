package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
)

type RssFeed struct {
	Id              string `json:"_id"`
	RssFeedName     string `json:"RSS_Feed_Name"`
	RssFeedUrl      string `json:"RSS_Feed_URL"`
	LastUpdatedDate string `json:"Last_Updated_Date"`
	Magazine        string `json:"Magazine"`
	Language        string `json:"Language"`
	PauseIngestion  bool   `json:"Pause_Ingestion"`
}

type Feed struct {
	Publisher       string `json:"publisher"`
	FeedUrl         string `json:"feed_url"`
	LastUpdatedDate string `json:"last_updated_date"`
	FeedName        string `json:"feed_name"`
	Language        string `json:"language"`
}

func main() {
	//TODO Update last updated date in Cloudant

	// Get the namespace we're in so we know how to talk to the Function
	file := "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	namespace, err := ioutil.ReadFile(file)
	if err != nil || len(namespace) == 0 {
		fmt.Fprintf(os.Stderr, "Missing namespace: %s\n%s\n", err, namespace)
		os.Exit(1)
	}

	// Query Cloudant for the feed list
	// selector= {"_id": {"$gt": "0"},"Publisher_Name": {"$exists": True},"RSS_Feeds": {"$exists": True}},
	service, err := cloudantv1.NewCloudantV1UsingExternalConfig(
		&cloudantv1.CloudantV1Options{},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing Cloudant Service: %s", err)
		os.Exit(1)
	}

	selector := map[string]interface{}{
		"_id": map[string]interface{}{
			"$gt": "0",
		},
		"Publisher_Name": map[string]interface{}{
			"$exists": true,
		},
		"RSS_Feeds": map[string]interface{}{
			"$exists": true,
		},
	}
	dbName := os.Getenv("db_name")
	queryOptions := &cloudantv1.PostFindOptions{
		Db:       &dbName,
		Selector: selector,
	}

	// Execute the query
	findResult, _, err := service.PostFind(queryOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error Finding All Documents using Cloudant Service: %s", err)
		os.Exit(1)
	}

	// Parse Result from Cloudant to build slice of RSS Feeds
	var feeds []Feed
	for _, doc := range findResult.Docs {
		var rssFeeds []RssFeed
		b, err := json.Marshal(doc.GetProperty("RSS_Feeds"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error Marshaling RSS_Feeds interface into JSON: %s", err)
			os.Exit(1)
		}
		err = json.Unmarshal(b, &rssFeeds)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error Decoding JSON: %s", err)
			os.Exit(1)
		}
		for _, rssfeed := range rssFeeds {
			if rssfeed.PauseIngestion == true {
				continue
			}
			feed := Feed{
				Publisher:       doc.GetProperty("Publisher_Name").(string),
				FeedUrl:         rssfeed.RssFeedUrl,
				FeedName:        rssfeed.RssFeedName,
				LastUpdatedDate: rssfeed.LastUpdatedDate,
				Language:        rssfeed.Language,
			}
			feeds = append(feeds, feed)
		}
	}

	count := len(feeds)
	fmt.Printf("Sending %d requests to Parse Feeds...\n", count)
	wg := sync.WaitGroup{}

	// URL to the Function
	url := "http://parse-feed." + string(namespace) + ".svc.cluster.local"

	// Do all requests to the Parse Feed Function in parallel - why not?
	for i := 0; i < count; i++ {
		payloadJson, _ := json.Marshal(feeds[i])
		wg.Add(1)
		go func(i int, payloadJson []byte) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				res, err := http.Post(url, "application/json", bytes.NewBuffer(payloadJson))

				if err == nil && res.StatusCode/100 == 2 {
					break
				}

				// Something went wrong, pause and try again
				body := []byte{}
				if res != nil {
					body, _ = ioutil.ReadAll(res.Body)
				}
				fmt.Fprintf(os.Stderr, "%d: err: %s\nhttp res: %#v\nbody:%s",
					i, err, res, string(body))
				time.Sleep(time.Second)
			}
		}(i, payloadJson)
	}

	// Wait for all threads to finish before we exit
	wg.Wait()

	fmt.Printf("Done\n")
}
