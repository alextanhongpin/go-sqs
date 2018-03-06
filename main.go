package main

import (
	"bytes"
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// 6625//3777
// 4827 - 4302
// 3579 / 4302
// 2129 / 4302

func Per(eventCount int, duration time.Duration) rate.Limit {
	return rate.Every(duration / time.Duration(eventCount))
}

var (
	client                  *http.Client
	MaxNumberOfMessages     int64 = 10
	WaitTimeSeconds         int64 = 0
	VisibilityTimeoutSecond int64 = 30

	QueueURL           string
	ContentBuilderURL  string
	AWSAccessKeyID     string
	AWSSecretAccessKey string
)

func main() {

	// Initialize global http client to reuse connection
	transport := &http.Transport{
		MaxIdleConnsPerHost: 20,
		TLSHandshakeTimeout: 0 * time.Second,
	}
	client = &http.Client{Transport: transport}

	var counter int64
	var mu sync.Mutex

	// 75,530 10m0.985324667s 6590
	// 69,146 1m0.548205912s 1930
	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("ap-southeast-1"),
		Credentials: credentials.NewStaticCredentials(AWSAccessKeyID, AWSSecretAccessKey, ""),
	})
	if err != nil {
		log.Println(err)
	}

	svc := sqs.New(sess)
	buf := make(chan interface{}, 1000)

	start := time.Now()
	// Set 1000/s
	limiter := rate.NewLimiter(Per(100, time.Second), 1)

	for {
		params := &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(QueueURL),
			MaxNumberOfMessages: aws.Int64(MaxNumberOfMessages),
			MessageAttributeNames: []*string{
				aws.String("All"),
			},
			VisibilityTimeout: aws.Int64(VisibilityTimeoutSecond), // In seconds
			WaitTimeSeconds:   aws.Int64(WaitTimeSeconds),
		}

		resp, err := svc.ReceiveMessage(params)
		if err != nil {
			log.Println(err)
			return
		}
		if len(resp.Messages) > 0 {
			if err := limiter.Wait(ctx); err != nil {
				log.Println("limiter err:", err)
			}
			go func() {

				buf <- struct{}{}
				doWork(svc, resp.Messages, buf)
				// <-buf
				mu.Lock()
				counter += int64(len(resp.Messages))
				mu.Unlock()
			}()

		}

		select {
		case <-ctx.Done():
			time.Sleep(30 * time.Second)
			now := time.Now()
			log.Println("cooldown period")
			log.Println("done:", time.Since(now-start), counter)
			return
		default:
		}
	}
}

func doWork(svc *sqs.SQS, messages []*sqs.Message, buf chan interface{}) bool {
	// numMessages := len(messages)
	// log.Printf("Received %d messages\n", numMessages)

	// var wg sync.WaitGroup
	// wg.Add(numMessages)
	for i := range messages {
		go func(m *sqs.Message) {
			// defer wg.Done()
			if err := handleMessage(svc, m); err != nil {
				log.Println("error handling message:", err.Error())
			}
			<-buf
		}(messages[i])
	}
	// wg.Wait()
	return true
}

func handleMessage(svc *sqs.SQS, m *sqs.Message) error {
	// POST
	req, err := http.NewRequest("POST", ContentBuilderURL, bytes.NewBuffer([]byte(*m.Body)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	// _ = body
	if err != nil {
		log.Println("err:", err)
		return err
	}
	log.Println(string(body))
	resp.Body.Close()

	// Delete message
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(QueueURL),
		ReceiptHandle: m.ReceiptHandle,
	}
	_, err = svc.DeleteMessage(params)
	if err != nil {
		return err
	}
	log.Println("msg del")
	return nil
}
