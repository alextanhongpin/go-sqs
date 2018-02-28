package main

import (
	"bytes"
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func Per(eventCount int, duration time.Duration) rate.Limit {
	return rate.Every(duration / time.Duration(eventCount))
}

var (
	MaxNumberOfMessages     int64 = 10
	WaitTimeSeconds         int64 = 0
	VisibilityTimeoutSecond int64 = 30

	QueueURL           string
	ContentBuilderURL  string
	AWSAccessKeyID     string
	AWSSecretAccessKey string
)

func main() {

	QueueURL = os.Getenv("QUEUE_URL")
	ContentBuilderURL = os.Getenv("CONTENT_BUILDER_URL")
	AWSAccessKeyID = os.Getenv("AWS_ACCESS_KEY")
	AWSSecretAccessKey = os.Getenv("AWS_SECRET_KEY")

	var counter int64
	var mu sync.Mutex
	// 75,530 10m0.985324667s 6590
	// 69,146 1m0.548205912s 1930
	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("ap-southeast-1"),
		Credentials: credentials.NewStaticCredentials(AWSAccessKeyID, AWSSecretAccessKey, ""),
	})
	if err != nil {
		log.Println(err)
	}

	svc := sqs.New(sess)
	buf := make(chan interface{}, 20)

	start := time.Now()
	// Set 1000/s
	limiter := rate.NewLimiter(Per(1000, time.Second), 1)

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
			mu.Lock()
			counter += int64(len(resp.Messages))
			mu.Unlock()
			buf <- struct{}{}
			go func() {
				if err := limiter.Wait(ctx); err != nil {
					log.Println("limiter err:", err)
				}
				doWork(svc, resp.Messages)
				<-buf
			}()

		}

		select {
		case <-ctx.Done():
			log.Println("done:", time.Since(start), counter)
			return
		default:
		}
	}
}

func doWork(svc *sqs.SQS, messages []*sqs.Message) bool {
	numMessages := len(messages)
	log.Printf("Received %d messages\n", numMessages)

	var wg sync.WaitGroup
	wg.Add(numMessages)
	for i := range messages {
		go func(m *sqs.Message) {
			defer wg.Done()
			if err := handleMessage(svc, m); err != nil {
				log.Println("error handling message:", err.Error())
			}
		}(messages[i])
	}
	wg.Wait()
	return true
}

func handleMessage(svc *sqs.SQS, m *sqs.Message) error {
	// POST
	req, err := http.NewRequest("POST", ContentBuilderURL, bytes.NewBuffer([]byte(*m.Body)))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// resp.Status

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("err:", err)
		return err
	}
	log.Println(string(body))

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
