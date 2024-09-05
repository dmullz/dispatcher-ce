package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
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
	ErrorCount      int64  `json:"Error_Count"`
}

type Feed struct {
	Publisher       string `json:"publisher"`
	FeedUrl         string `json:"feed_url"`
	LastUpdatedDate string `json:"last_updated_date"`
	FeedName        string `json:"feed_name"`
	Language        string `json:"language"`
	ErrorCount      int64  `json:"error_count"`
}

type FeedPayload struct {
	FeedList []Feed `json:"feed_list"`
}

type ParsedData struct {
	Body         []byte
	ParsedStatus int
	FeedStatus   FeedStatus
}

type ParseFeedRes struct {
	//ParsedFeed   []byte `json:"parsed_feed"`
	ErrorParsing int64 `json:"error_parsing"`
}

type DufRes struct {
	Leads      []string `json:"leads"`
	ErrorCount int      `json:"error_count"`
	Publisher  string   `json:"publisher"`
	Magazine   string   `json:"magazine"`
}

type LeadsData struct {
	Leads        []string
	DownUpStatus int
	ErrorCount   int
	Publisher    string
	Magazine     string
}

type LBAResults struct {
	ArticleId           string
	LeadByArticleStatus int
}

type FeedStatus struct {
	Feed             Feed
	ErrorParsing     int64
	ErrorDownloading int
}

type BrevoSender struct {
	Name  string `json:"sender"`
	Email string `json:"email"`
}

type BrevoTo struct {
	Email string `json:"email"`
}

type BrevoQuery struct {
	Sender      BrevoSender `json:"sender"`
	To          []BrevoTo   `json:"to"`
	Subject     string      `json:"subject"`
	HtmlContent string      `json:"htmlContent"`
	Bcc         []BrevoTo   `json:"bcc"`
}

type SFAccessTokenRes struct {
	AccessToken string `json:"access_token"`
}

type SFCSMObject struct {
	Email string `json:"email"`
}

type SFQueryRecord struct {
	ClientSuccessManager SFCSMObject `json:"Client_Success_Manager__r"`
}

type SFQueryRes struct {
	Records   []SFQueryRecord `json:"records"`
	TotalSize int             `json:"totalSize"`
	Done      bool            `json:"done"`
}

func main() {

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
				ErrorCount:      rssfeed.ErrorCount,
			}
			feeds = append(feeds, feed)
		}
	}

	count := len(feeds)
	var allFeedStatuses []FeedStatus
	fmt.Printf(os.Getenv("env")+" Sending %d requests to Parse Feeds...\n", count)

	// URL to the Function
	pfUrl := os.Getenv("parse_feed_url")

	// Run Dispatcher for every feed in batches of 200

	startIndex := 0
	batchCount := 1
	var allParsedData [][]byte
	numErrors := 0
	for startIndex < count {
		wgPF := sync.WaitGroup{}

		// Create channel to store Parse Feed responses
		parsedDataCh := make(chan ParsedData, min(200, count-startIndex))

		i := startIndex
		for i < startIndex+200 && i < count {
			var payloadFeeds []Feed
			payloadFeeds = append(payloadFeeds, feeds[i])

			feedPayload := FeedPayload{
				FeedList: payloadFeeds,
			}
			payloadJson, _ := json.Marshal(feedPayload)
			wgPF.Add(1)
			go func(i int, payloadJson []byte, feed Feed) {
				defer wgPF.Done()
				sleep := 1
				for j := 0; j < 10; j++ {
					res, err := http.Post(pfUrl, "application/json", bytes.NewBuffer(payloadJson))

					if err == nil && (res.StatusCode == 200 || res.StatusCode == 202) {
						body := []byte{}
						body, _ = ioutil.ReadAll(res.Body)
						//if os.Getenv("env") == "DEV" {
						//	fmt.Printf("RAW PARSE FEED BODY: %s\n", string(body))
						//}
						var parseFeedRes ParseFeedRes
						err := json.NewDecoder(bytes.NewBuffer(body)).Decode(&parseFeedRes)
						if err != nil {
							fmt.Printf("JSON decode for PARSE FEED RESPONSE error! Error: %s\n", err)
						}
						//if os.Getenv("env") == "DEV" {
						//	fmt.Printf("DECODED ErrorParsing Value: %d\n", parseFeedRes.ErrorParsing)
						//}
						feedStatus := FeedStatus{
							Feed:             feed,
							ErrorParsing:     parseFeedRes.ErrorParsing,
							ErrorDownloading: 0,
						}
						parsedData := ParsedData{
							Body:         body,
							ParsedStatus: 0,
							FeedStatus:   feedStatus,
						}
						parsedDataCh <- parsedData

						break
					}

					// Something went wrong, pause and try again
					body := []byte{}
					if res != nil {
						body, _ = ioutil.ReadAll(res.Body)
					}
					if j >= 9 {
						// Done retries, store error and exit
						fmt.Fprintf(os.Stderr, os.Getenv("env")+" Thread #: (%d); err: (%s); status: (%d); body: (%s); payload: (%s)\n",
							i, err, res.StatusCode, string(body), string(payloadJson))
						parsedData := ParsedData{
							Body:         body,
							ParsedStatus: 1,
						}
						parsedDataCh <- parsedData
						break
					}
					//fmt.Fprintf(os.Stderr, os.Getenv("env")+" Thread #: (%d); err: (%s); status: (%d); body: (%s)\n",
					//	i, err, res.StatusCode, string(body))
					time.Sleep(time.Second * time.Duration(sleep))
					sleep *= 2
				}
			}(i, payloadJson, feeds[i])
			i++
		}

		// Wait for all threads to finish before we exit
		wgPF.Wait()
		close(parsedDataCh)

		// Gather Data From Channel
		for chValue := range parsedDataCh {
			if chValue.ParsedStatus == 0 {
				allParsedData = append(allParsedData, chValue.Body)
				allFeedStatuses = append(allFeedStatuses, chValue.FeedStatus)
			} else {
				numErrors++
			}
		}

		fmt.Printf(os.Getenv("env")+" Parse Feed Batch #: %d Completed with %d Errors so far\n", batchCount, numErrors)

		startIndex = startIndex + 200
		batchCount++
	}
	fmt.Printf(os.Getenv("env") + " Done Dispatching Feeds\n")

	fmt.Printf(os.Getenv("env")+" Dispatched %d feeds with %d Errors\n", count, numErrors)

	count = len(allParsedData)
	fmt.Printf(os.Getenv("env")+" Sending %d requests to Download Upload Feed...\n", count)

	// URL to the Function
	dufUrl := os.Getenv("download_upload_url")

	// Run Download-Upload for every feed in batches of 200

	startIndex = 0
	batchCount = 1
	numErrors = 0
	var allLeads []string
	for startIndex < count {
		wgDUF := sync.WaitGroup{}

		// Create channel to store Download Upload responses
		leadsDataCh := make(chan LeadsData, min(200, count-startIndex))

		i := startIndex
		for i < startIndex+200 && i < count {
			wgDUF.Add(1)
			go func(i int, payloadJson []byte) {
				defer wgDUF.Done()
				sleep := 1
				for j := 0; j < 10; j++ {
					res, err := http.Post(dufUrl, "application/json", bytes.NewBuffer(payloadJson))

					if err == nil && res.StatusCode/100 == 2 {
						var dufRes DufRes
						err := json.NewDecoder(res.Body).Decode(&dufRes)
						if err != nil {
							fmt.Println("JSON decode for DOWNLOAD UPLOAD FEED RESPONSE error! Response Code: %d", res.StatusCode)
							leadsData := LeadsData{
								Leads:        nil,
								DownUpStatus: 1,
							}
							leadsDataCh <- leadsData
							break
						}
						leadsData := LeadsData{
							Leads:        dufRes.Leads,
							DownUpStatus: 0,
							ErrorCount:   dufRes.ErrorCount,
							Publisher:    dufRes.Publisher,
							Magazine:     dufRes.Magazine,
						}
						leadsDataCh <- leadsData
						break
					}

					// Something went wrong, pause and try again
					body := []byte{}
					if res != nil {
						body, _ = ioutil.ReadAll(res.Body)
					}
					if j >= 9 {
						// Done retries, store error in Channel and exit
						fmt.Fprintf(os.Stderr, os.Getenv("env")+" Thread #: (%d); err: (%s); status: (%d); body: (%s); payload size: (%d)\n",
							i, err, res.StatusCode, string(body), len(payloadJson))
						leadsData := LeadsData{
							Leads:        nil,
							DownUpStatus: 1,
						}
						leadsDataCh <- leadsData
						break
					}
					time.Sleep(time.Second * time.Duration(sleep))
					sleep *= 2
				}
			}(i, allParsedData[i])
			i++
		}

		// Wait for all threads to finish before we exit
		wgDUF.Wait()
		close(leadsDataCh)

		// Gather Data From Channel
		for chValue := range leadsDataCh {
			if chValue.DownUpStatus == 0 {
				for _, lead := range chValue.Leads {
					allLeads = append(allLeads, lead)
				}
				if chValue.ErrorCount > 0 {
					fmt.Printf(os.Getenv("env")+" Error downloading articles from Publisher: %s, Magazine: %s, Error Count: %d\n", chValue.Publisher, chValue.Magazine, chValue.ErrorCount)
					for f := range allFeedStatuses {
						if allFeedStatuses[f].Feed.Publisher == chValue.Publisher && allFeedStatuses[f].Feed.FeedName == chValue.Magazine {
							allFeedStatuses[f].ErrorDownloading = chValue.ErrorCount
						}
					}
				}
			} else {
				numErrors++
			}
		}

		fmt.Printf(os.Getenv("env")+" Download-Upload Batch #: %d Completed with %d Errors so far\n", batchCount, numErrors)

		startIndex = startIndex + 200
		batchCount++
	}

	fmt.Printf(os.Getenv("env") + " Done Downloading/Uploading Feeds\n")

	fmt.Printf(os.Getenv("env")+" Dispatched %d download/upload threads with %d Errors\n", count, numErrors)

	count = len(allLeads)
	fmt.Printf(os.Getenv("env")+" Creating %d Leads...\n", count)

	// URL to the Function
	lbaUrl := os.Getenv("lead_by_article_url")

	// Run Lead by article for every lead in batches of 200

	startIndex = 0
	batchCount = 1
	totalLeadsCreated := 0
	for startIndex < count {
		wgLBA := sync.WaitGroup{}
		// Create channel to store Lead By Article responses
		lbaResCh := make(chan LBAResults, min(200, count-startIndex))
		i := startIndex
		for i < startIndex+200 && i < count {
			params := url.Values{}
			params.Add("article_id", allLeads[i])
			fullURL := lbaUrl + "?" + params.Encode()
			wgLBA.Add(1)
			go func(i int, articleId string, fullURL string) {
				defer wgLBA.Done()
				for j := 0; j < 10; j++ {
					res, err := http.Get(fullURL)

					if err == nil && res.StatusCode/100 == 2 {
						lbaResults := LBAResults{
							ArticleId:           articleId,
							LeadByArticleStatus: 0,
						}
						lbaResCh <- lbaResults
						break
					}

					// Something went wrong, pause and try again
					body := []byte{}
					if res != nil {
						body, _ = ioutil.ReadAll(res.Body)
					}

					if j >= 9 {
						// Done retries, store error in Channel and exit
						fmt.Fprintf(os.Stderr, os.Getenv("env")+" Thread #: (%d); err: (%s); status: (%d); body: (%s); article_id: (%s)\n",
							i, err, res.StatusCode, string(body), articleId)
						lbaResults := LBAResults{
							ArticleId:           articleId,
							LeadByArticleStatus: 1,
						}
						lbaResCh <- lbaResults
						break
					}
					time.Sleep(time.Second)
				}
			}(i, allLeads[i], fullURL)
			i++
		}

		// Wait for all threads to finish
		wgLBA.Wait()
		close(lbaResCh)
		fmt.Printf(os.Getenv("env")+" Done Batch %d of Lead By Article\n", batchCount)

		// Gather Data From Channel
		numErrors = 0
		numLeads := 0
		for chValue := range lbaResCh {
			if chValue.LeadByArticleStatus == 1 {
				numErrors++
				fmt.Printf(os.Getenv("env")+" Unable to create lead for article with ID: %s\n", chValue.ArticleId)
			} else {
				numLeads++
			}
		}

		fmt.Printf(os.Getenv("env")+" Batch #: %d; Created %d leads with %d Errors\n", batchCount, numLeads, numErrors)
		totalLeadsCreated = totalLeadsCreated + numLeads

		startIndex = startIndex + 200
		batchCount++
	}

	fmt.Printf(os.Getenv("env")+" Done. %d Leads Created\n", totalLeadsCreated)

	fmt.Printf(os.Getenv("env") + " Collating errors and sending out alerts if needed\n")

	//Loop through Cloudant docs and update values as necessary
	var emailFeeds []FeedStatus
	for d := range findResult.Docs {
		var newrssFeeds []RssFeed
		b, err := json.Marshal(findResult.Docs[d].GetProperty("RSS_Feeds"))
		if err != nil {
			fmt.Fprintf(os.Stderr, os.Getenv("env")+" Error Marshaling RSS_Feeds interface into JSON: %s\n", err)
			os.Exit(1)
		}
		err = json.Unmarshal(b, &newrssFeeds)
		if err != nil {
			fmt.Fprintf(os.Stderr, os.Getenv("env")+" Error Decoding JSON: %s\n", err)
			os.Exit(1)
		}
		for i := range newrssFeeds {
			for _, feedStatus := range allFeedStatuses {
				if findResult.Docs[d].GetProperty("Publisher_Name").(string) == feedStatus.Feed.Publisher && newrssFeeds[i].RssFeedName == feedStatus.Feed.FeedName {
					todayDate := time.Now()
					todayString := todayDate.Format("2006-01-02 15:04:05")
					newrssFeeds[i].LastUpdatedDate = todayString
					if feedStatus.ErrorParsing != -1 || feedStatus.ErrorDownloading != 0 {
						fmt.Printf(os.Getenv("env")+" Incrementing Feed %s current ErrorCount %d due to error. ErrorParsing: %d, ErrorDownloading: %d\n", feedStatus.Feed.FeedName, newrssFeeds[i].ErrorCount, feedStatus.ErrorParsing, feedStatus.ErrorDownloading)
						feedStatus.Feed.ErrorCount = feedStatus.Feed.ErrorCount + 1
						newrssFeeds[i].ErrorCount = newrssFeeds[i].ErrorCount + 1
						if os.Getenv("env") == "DEV" {
							fmt.Printf(os.Getenv("env")+" New ErrorCount for Feed %s : %d\n", feedStatus.Feed.FeedName, newrssFeeds[i].ErrorCount)
						}
						if feedStatus.Feed.ErrorCount > 3 {
							newrssFeeds[i].PauseIngestion = true
							emailFeeds = append(emailFeeds, feedStatus)
						}
					} else {
						if os.Getenv("env") == "DEV" {
							fmt.Printf(os.Getenv("env")+" Setting ErrorCount to 0 for Feed %s as there were no errors. ErrorParsing: %d, ErrorDownloading: %d\n", feedStatus.Feed.FeedName, feedStatus.ErrorParsing, feedStatus.ErrorDownloading)
						}
						newrssFeeds[i].ErrorCount = 0
					}
				}
			}
		}

		if os.Getenv("env") == "DEV" {
			for _, rssfeed := range newrssFeeds {
				fmt.Printf(os.Getenv("env")+" Updating Feed %s info in Cloudant with date: %s\n", rssfeed.RssFeedName, rssfeed.LastUpdatedDate)
			}
		}

		//Update RSS_Feeds in doc with latest changes
		rssFeedJson, _ := json.Marshal(newrssFeeds)
		var stringInterfaceMapJson []map[string]interface{}
		err = json.Unmarshal(rssFeedJson, &stringInterfaceMapJson)
		if err != nil {
			panic(err)
		}
		findResult.Docs[d].SetProperty("RSS_Feeds", stringInterfaceMapJson)
	}

	//if os.Getenv("env") == "DEV" {
	//	for _, doc := range findResult.Docs {
	//		jsonOutput, _ := json.Marshal(doc.GetProperty("RSS_Feeds"))
	//		fmt.Printf(os.Getenv("env")+" Updating Feed info in Cloudant: %s\n", string(jsonOutput))
	//	}
	//}

	//Bulk update Cloudant with changes
	postBulkDocsOptions := service.NewPostBulkDocsOptions(
		dbName,
	)
	bulkDocs, err := service.NewBulkDocs(
		findResult.Docs,
	)
	if err != nil {
		panic(err)
	}
	postBulkDocsOptions.SetBulkDocs(bulkDocs)

	_, _, err = service.PostBulkDocs(postBulkDocsOptions)
	if err != nil {
		panic(err)
	}
	fmt.Printf(os.Getenv("env") + " Done updating Cloudant with latest ErrorCount\n")

	if len(emailFeeds) > 0 {
		err = SendEmails(emailFeeds)
		if err != nil {
			panic(err)
		}
		fmt.Printf(os.Getenv("env")+" Done Sending Emails for %d feeds Containing Errors\n", len(emailFeeds))
	}

	fmt.Printf(os.Getenv("env") + " Done Ingestion.")
}

func SendEmails(emailFeeds []FeedStatus) error {
	// Get Salesforce Access Token
	sf_token, err := GetToken()
	if err != nil {
		fmt.Println("Error getting Access Token for SalesForce:", err)
		return err
	}

	csmEmailFeed := make(map[string][]FeedStatus)

	for _, emailFeed := range emailFeeds {
		sfQueryRes, err := QuerySalesForce(sf_token, emailFeed.Feed.FeedName)
		if err != nil {
			fmt.Println("Error Querying SalesForce:", err)
			return err
		}
		if sfQueryRes.TotalSize != 1 {
			fmt.Printf("Error: Client Success Manager Query has invalid size of %d\n", sfQueryRes.TotalSize)
			return errors.New("Invalid Query Reponse Size")
		}
		csmEmailFeed[sfQueryRes.Records[0].ClientSuccessManager.Email] = append(csmEmailFeed[sfQueryRes.Records[0].ClientSuccessManager.Email], emailFeed)
	}

	for email := range csmEmailFeed {
		err := SendEmail(email, csmEmailFeed[email])
		if err != nil {
			fmt.Println("Error sending email containing feed ingestion errors", err)
			return err
		}
	}
	return nil

}

func GetToken() (string, error) {
	params := url.Values{}
	params.Add("grant_type", "refresh_token")
	params.Add("client_id", os.Getenv("RF_KEY"))
	params.Add("client_secret", os.Getenv("RF_SECRET"))
	params.Add("refresh_token", os.Getenv("RF_TOKEN"))
	fullURL := os.Getenv("RF_URL") + "?" + params.Encode()
	res, err := http.Get(fullURL)
	if err == nil && res.StatusCode/100 == 2 {
		var sfAccessTokenRes SFAccessTokenRes
		err := json.NewDecoder(res.Body).Decode(&sfAccessTokenRes)
		if err != nil {
			fmt.Println("Error Decoding SalesForce Access Token JSON Response:", err)
			return "", err
		}
		return sfAccessTokenRes.AccessToken, nil
	}
	return "", err
}

func QuerySalesForce(sf_token string, magazine string) (*SFQueryRes, error) {
	// Query Salesforce for client success manager email
	client := &http.Client{}
	params := url.Values{}
	params.Add("q", "SELECT Client_Success_Manager__r.Email from Magazine__c where Name like '"+magazine+"'")
	fullURL := os.Getenv("SF_URL") + "v61.0/query/?" + params.Encode()
	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		fmt.Printf("Error creating HTTP request to Salesforce: %s", err)
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+sf_token)
	req.Header.Set("Content-Type", "application/json")
	req.Close = true
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error:", err)
		return nil, err
	}
	defer resp.Body.Close()
	var sfQueryRes SFQueryRes
	err = json.NewDecoder(resp.Body).Decode(&sfQueryRes)
	if err != nil {
		fmt.Printf("Error Decoding SalesForce Query JSON Response for magazine: %s FullURL: %s Error: %s\n", magazine, fullURL, err)
		return nil, err
	}
	return &sfQueryRes, nil
}

func SendEmail(email string, emailFeeds []FeedStatus) error {
	//Send email notifying Client Success Manager of Fails using brevo

	email_body := ""
	//email_body = email_body + "Client Success Manager email for this feed is: " + email + ".<br><br><br>"
	for _, emailFeed := range emailFeeds {
		error_message := ""
		if emailFeed.ErrorParsing != -1 {
			switch emailFeed.ErrorParsing {
			case 0:
				error_message = error_message + "<li>RSS Feed is empty or Feed URL does not contain any readable Feed XML.</li>"
			case 401:
				error_message = error_message + "<li>Ingestion Application is not Authorized to read this RSS Feed.</li>"
			case 403:
				error_message = error_message + "<li>RSS Feed has forbidden access to the Ingestion Application.</li>"
			case 404:
				error_message = error_message + "<li>RSS Feed cannot be found.</li>"
			case 500:
				error_message = error_message + "<li>RSS Feed XML is in an unknown format and cannot be read.</li>"
			default:
				error_message = error_message + "<li>Unknown Error Reading RSS Feed.</li>"
			}
		}
		if emailFeed.ErrorDownloading != 0 {
			error_message = error_message + "<li>Ingestion Application could not download article text from " + strconv.Itoa(emailFeed.ErrorDownloading) + " articles.</li>"
		}
		email_body = email_body + "The feed for <b>" + emailFeed.Feed.FeedName + "</b> (" + emailFeed.Feed.Publisher + ") has had 3 successive errors. Ingestion has been paused for this feed.<br><br>URL: <a href='" + emailFeed.Feed.FeedUrl + "'>" + emailFeed.Feed.FeedUrl + "</a><br><br>Errors Include:<ul>" + error_message + "</ul><br><br><br>"
	}

	client := &http.Client{}
	var toList []BrevoTo
	toList = append(toList, BrevoTo{Email: os.Getenv("email_address")})
	toList = append(toList, BrevoTo{Email: email})
	var bccList []BrevoTo
	bccList = append(bccList, BrevoTo{Email: "david.mullen.085@gmail.com"})
	payload := BrevoQuery{
		Sender: BrevoSender{
			Name:  "RSS Mailer",
			Email: "WM.RSS.mailer@gmail.com",
		},
		To:          toList,
		Bcc:         bccList,
		Subject:     "Unable to Ingest Articles from Feed",
		HtmlContent: "<html><head></head><body>" + email_body + "<br><br><br>WM RSS Mailer</body></html>",
	}
	payloadJson, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", "https://api.brevo.com/v3/smtp/email", bytes.NewBuffer(payloadJson))
	if err != nil {
		fmt.Printf("Error creating HTTP request to Brevo: %s\n", err)
		return err
	}
	req.Header.Set("api-key", os.Getenv("brevo_api_key"))
	req.Close = true
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error:", err)
		return err
	}
	defer resp.Body.Close()
	return nil
}
