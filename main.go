package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
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
	client                  *http.Client
	mu                      sync.Mutex
	start                   time.Time
	counter                 int64
	waitTimeSeconds         int64
	maxNumberOfMessages     int64 = 10
	visibilityTimeoutSecond int64 = 30

	queueURL           string
	contentBuilderURL  string
	awsAccessKeyID     string
	awsSecretAccessKey string
)

// 	13,632 - 11192 - 2440 in 2 minutes, 20 req/s, pool 5, limiter 2 per second
// 11192 - 5,382 - 5810 in 2 minutes, 48.4 req/s, pool 5, limiter 5 per second
// 5382 - 2145 - 3237 in  2 minutes, 17 req/s, pool 5, limiter 10 per second
// 2145 - 1045 - 1100 in 2 minutes, 9 req/s, pool 10, limiter 5 per second
// 30,992 - 28,713: 2276 in 2 minutes 18.9 req/s pool 5, limiter 50 per second
// 28,713
func main() {

	// Logging the number of existing goroutines
	go func() {
		for {
			time.Sleep(1 * time.Second)
			log.Printf("#goroutines: %d\n", runtime.NumGoroutine())
		}
	}()

	// Initialize global http client to reuse connection
	transport := &http.Transport{
		MaxIdleConnsPerHost: 1024,
		TLSHandshakeTimeout: 0 * time.Second,
	}
	client = &http.Client{Transport: transport}

	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("ap-southeast-1"),
		Credentials: credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, ""),
	})
	if err != nil {
		log.Println(err)
	}

	svc := sqs.New(sess)
	pool := make(chan interface{}, 10)
	pause := make(chan interface{}, 1)

	// Limit to 5 invocations per second - each invocation will fetch n amount of messages.
	// If n number of messages is 10, you can fetch up to 50 messages in a second,
	// and the buffer limit only 50 in-flight messages to be available
	limiter := rate.NewLimiter(Per(50, time.Second), 0)

	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: aws.Int64(maxNumberOfMessages),
		MessageAttributeNames: []*string{
			aws.String("All"),
		},
		VisibilityTimeout: aws.Int64(visibilityTimeoutSecond), // In seconds
		WaitTimeSeconds:   aws.Int64(waitTimeSeconds),
	}

	start = time.Now()

	for {
		resp, err := svc.ReceiveMessage(params)
		if err != nil {
			log.Println(err)
			return
		}

		select {
		case <-ctx.Done():
			log.Println("#done, pending for remaining task to complete")
			time.Sleep(1 * time.Minute)
			log.Println("#end:", time.Since(start), counter)
			return
		default:
			if len(resp.Messages) > 0 {
				if err := limiter.Wait(ctx); err != nil {
					log.Println("limiter err:", err)
					time.Sleep(1 * time.Second)
				}
				select {
				case <-pause:
					log.Println("#pausing for 10 seconds")
					time.Sleep(10 * time.Second)
					log.Println("#resume")
					go doWork(svc, resp.Messages, pool, pause)
				default:
					go doWork(svc, resp.Messages, pool, pause)
				}

			} else {
				log.Println("#nomsg, waiting 1 minute")
				time.Sleep(1 * time.Minute)
				log.Println("#end:", time.Since(start), counter)
			}
		}
	}
}

func doWork(svc *sqs.SQS, msgs []*sqs.Message, pool chan interface{}, pause chan interface{}) {
	for i := range msgs {
		pool <- struct{}{}
		go func(m *sqs.Message) {
			if err := handleMessage(svc, m); err != nil {
				log.Println("error handling message:", err.Error())
				pause <- struct{}{}
			} else {
				mu.Lock()
				counter += int64(1)
				mu.Unlock()
				log.Printf("%0.0f req/s", float64(counter)/time.Since(start).Seconds())
			}
			<-pool
		}(msgs[i])
	}
}

func handleMessage(svc *sqs.SQS, m *sqs.Message) error {
	// POST
	req, err := http.NewRequest("POST", ContentBuilderURL, bytes.NewBuffer([]byte(*m.Body)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("got %v", resp.StatusCode)
	}

	if _, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Println("err:", err)
		return err
	}

	// Delete message
	if _, err = svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(QueueURL),
		ReceiptHandle: m.ReceiptHandle,
	}); err != nil {
		return err
	}

	return nil
}
