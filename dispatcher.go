package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"

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

type FeedPayload struct {
	FeedList []Feed `json:"feed_list"`
}

type ParsedData struct {
	Body         []byte
	ParsedStatus int
}

type DufRes struct {
	Leads []string `json:"leads"`
}

type LeadsData struct {
	Leads        []string
	DownUpStatus int
}

func main() {
	//TODO Update last updated date in Cloudant

	// Get the namespace we're in so we know how to talk to the Function
	file := "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	namespace, err := ioutil.ReadFile(file)
	if err != nil || len(namespace) == 0 {
		fmt.Fprintf(os.Stderr, "Missing namespace: %s; %s\n", err, namespace)
		os.Exit(1)
	}

	// Query Cloudant for the feed list
	// selector= {"_id": {"$gt": "0"},"Publisher_Name": {"$exists": True},"RSS_Feeds": {"$exists": True}},
	service, err := cloudantv1.NewCloudantV1UsingExternalConfig(
		&cloudantv1.CloudantV1Options{},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, os.Getenv("env")+" Error initializing Cloudant Service: %s\n", err)
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
		fmt.Fprintf(os.Stderr, os.Getenv("env")+" Error Finding All Documents using Cloudant Service: %s\n", err)
		os.Exit(1)
	}

	// Parse Result from Cloudant to build slice of RSS Feeds
	var feeds []Feed
	for _, doc := range findResult.Docs {
		var rssFeeds []RssFeed
		b, err := json.Marshal(doc.GetProperty("RSS_Feeds"))
		if err != nil {
			fmt.Fprintf(os.Stderr, os.Getenv("env")+" Error Marshaling RSS_Feeds interface into JSON: %s\n", err)
			os.Exit(1)
		}
		err = json.Unmarshal(b, &rssFeeds)
		if err != nil {
			fmt.Fprintf(os.Stderr, os.Getenv("env")+" Error Decoding JSON: %s\n", err)
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
	fmt.Printf(os.Getenv("env")+" Sending %d requests to Parse Feeds...\n", count)
	wgPF := sync.WaitGroup{}

	// URL to the Function
	url := os.Getenv("parse_feed_url")

	// Create channel to store Parse Feed responses
	parsedDataCh := make(chan ParsedData, int(count/4)+count%4)

	i := 0
	for i < count {
		var payloadFeeds []Feed
		if i+3 >= count {
			payloadFeeds = append(payloadFeeds, feeds[i])
			i++
		} else {
			payloadFeeds = append(payloadFeeds, feeds[i])
			payloadFeeds = append(payloadFeeds, feeds[i+1])
			payloadFeeds = append(payloadFeeds, feeds[i+2])
			payloadFeeds = append(payloadFeeds, feeds[i+3])
			i = i + 4
		}

		feedPayload := FeedPayload{
			FeedList: payloadFeeds,
		}
		payloadJson, _ := json.Marshal(feedPayload)
		wgPF.Add(1)
		go func(i int, payloadJson []byte) {
			defer wgPF.Done()
			for j := 0; j < 10; j++ {
				res, err := http.Post(url, "application/json", bytes.NewBuffer(payloadJson))

				if err == nil && res.StatusCode/100 == 2 {
					body := []byte{}
					body, _ = ioutil.ReadAll(res.Body)
					parsedData := ParsedData{
						Body:         body,
						ParsedStatus: 0,
					}
					parsedDataCh <- parsedData
					break
				}

				// Something went wrong, pause and try again
				body := []byte{}
				if res != nil {
					body, _ = ioutil.ReadAll(res.Body)
				}
				fmt.Fprintf(os.Stderr, os.Getenv("env")+" Thread #: (%d); err: (%s); status: (%d); body: (%s)\n", i/4, err, res.StatusCode, string(body))
				//turn off retries for now
				parsedData := ParsedData{
					Body:         body,
					ParsedStatus: 1,
				}
				parsedDataCh <- parsedData
				break
				//time.Sleep(time.Second)
			}
		}(i, payloadJson)
	}

	// Wait for all threads to finish before we exit
	wgPF.Wait()
	close(parsedDataCh)
	fmt.Printf(os.Getenv("env") + " Done Dispatching Feeds\n")

	// Gather Data From Channel
	var allParsedData [][]byte
	numErrors := 0
	for chValue := range parsedDataCh {
		if chValue.ParsedStatus == 0 {
			allParsedData = append(allParsedData, chValue.Body)
		} else {
			numErrors++
		}
	}

	fmt.Printf(os.Getenv("env")+" Dispatched %d feeds with %d Errors\n", count, numErrors)

	count = len(allParsedData)
	fmt.Printf(os.Getenv("env")+" Sending %d requests to Download Upload Feed...\n", count)
	wgDUF := sync.WaitGroup{}

	// URL to the Function
	url = os.Getenv("download_upload_url")

	// Create channel to store Download Upload responses
	leadsDataCh := make(chan LeadsData, count)

	for i = 0; i < count; i++ {
		wgDUF.Add(1)
		go func(i int, payloadJson []byte) {
			defer wgDUF.Done()
			for j := 0; j < 10; j++ {
				res, err := http.Post(url, "application/json", bytes.NewBuffer(payloadJson))

				if err == nil && res.StatusCode/100 == 2 {
					var dufRes DufRes
					err := json.NewDecoder(res.Body).Decode(&dufRes)
					if err != nil {
						fmt.Println("JSON decode for DOWNLOAD UPLOAD FEED RESPONSE error!")
						panic(err)
					}
					leadsData := LeadsData{
						Leads:        dufRes.Leads,
						DownUpStatus: 0,
					}
					leadsDataCh <- leadsData
					break
				}

				// Something went wrong, pause and try again
				body := []byte{}
				if res != nil {
					body, _ = ioutil.ReadAll(res.Body)
				}
				fmt.Fprintf(os.Stderr, os.Getenv("env")+" Thread #: (%d); err: (%s); status: (%d); body: (%s)\n", i, err, res.StatusCode, string(body))
				//turn off retries for now
				leadsData := LeadsData{
					Leads:        nil,
					DownUpStatus: 1,
				}
				leadsDataCh <- leadsData
				break
				//time.Sleep(time.Second)
			}
		}(i, allParsedData[i])
	}

	// Wait for all threads to finish before we exit
	wgDUF.Wait()
	close(leadsDataCh)
	fmt.Printf(os.Getenv("env") + " Done Downloading/Uploading Feeds\n")

	// Gather Data From Channel
	var allLeads []string
	numErrors = 0
	for chValue := range leadsDataCh {
		if chValue.DownUpStatus == 0 {
			for _, lead := range chValue.Leads {
				allLeads = append(allLeads, lead)
			}
		} else {
			numErrors++
		}
	}

	fmt.Printf(os.Getenv("env")+" Dispatched %d download/upload threads with %d Errors\n", count, numErrors)

	count = len(allLeads)
	fmt.Printf(os.Getenv("env")+" Creating %d Leads...\n", count)

}
