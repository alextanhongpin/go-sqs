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

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("ap-southeast-1"),
		Credentials: credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, ""),
	})
	if err != nil {
		log.Println(err)
	}

	svc := sqs.New(sess)
	pool := make(chan interface{}, 5)
	pause := make(chan interface{}, 1)

	// Limit to 5 invocations per second - each invocation will fetch n amount of messages.
	// If n number of messages is 10, you can fetch up to 50 messages in a second,
	// and the buffer limit only 50 in-flight messages to be available
	limiter := rate.NewLimiter(Per(50, time.Second), 1)

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
		if err := limiter.Wait(ctx); err != nil {
			log.Println("limiter err:", err)
			// time.Sleep(1 * time.Second)
		}
		select {
		case _, ok := <-pause:
			if ok {
				log.Println("#pausing for 10 seconds")
				time.Sleep(5 * time.Second)
				log.Println("#resume")
			}
		case <-ctx.Done():
			log.Println("#done, pending for remaining task to complete")
			time.Sleep(1 * time.Minute)
			log.Println("#end:", time.Since(start), counter)
			return
		default:
		}
		// default:
		log.Println("default")
		resp, err := svc.ReceiveMessage(params)
		if err != nil {
			log.Println(err)
			return
		}
		if len(resp.Messages) > 0 {
			log.Println("got messages", len(resp.Messages))
			pool <- struct{}{}
			go doWork(svc, resp.Messages, pool, pause)
		} else {
			log.Println("#nomsg, waiting 1 minute")
			time.Sleep(1 * time.Minute)
			log.Println("#end:", time.Since(start), counter)
		}
		// }
	}
}

func doWork(svc *sqs.SQS, msgs []*sqs.Message, pool chan interface{}, pause chan interface{}) {
	var wg sync.WaitGroup
	wg.Add(len(msgs))

	for i := range msgs {
		go func(m *sqs.Message) {
			if err := handleMessage(svc, m, pause); err != nil {
				log.Println("error handling message:", err.Error())
			} else {
				mu.Lock()
				counter += int64(1)
				mu.Unlock()
				log.Printf("%0.0f req/s, count = %d, elapsed = %v", float64(counter)/time.Since(start).Seconds(), counter, time.Since(start))
			}
			wg.Done()
		}(msgs[i])
	}
	wg.Wait()
	<-pool
}

func handleMessage(svc *sqs.SQS, m *sqs.Message, pause chan interface{}) error {
	// POST
	req, err := http.NewRequest("POST", contentBuilderURL, bytes.NewBuffer([]byte(*m.Body)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("err:", err)
		return err
	}
	log.Println("ok", string(body))
	if resp.StatusCode >= 500 {
		pause <- struct{}{}
		return fmt.Errorf("got %v", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("got %v", resp.StatusCode)
	}

	// Delete message
	_, err = svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: m.ReceiptHandle,
	})

	if err != nil {
		return err
	}
	log.Println("del")
	return nil
}
