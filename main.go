package main

import (
	"fmt"
	"github.com/adrianosela/sqs-backed-s3-processing/processor"
	"github.com/aws/aws-sdk-go/aws/session"
	"io"
	"log"

	"time"
)

// NOTE: replace with real values of region, account, and name
const sqsQueueArn = "https://sqs.<QUEUE_AWS_REGION>.amazonaws.com/<QUEUE_AWS_ACCOUNT>/<QUEUE_NAME>"

func main() {
	p := processor.New(
		session.Must(session.NewSession()), // will use default profile from ~/.aws profile
		sqsQueueArn,
		uint8(5), // n workers
		uint8(1)) // batch size

	// stop in 5 seconds
	go func() {
		time.Sleep(time.Second * 5)
		p.Stop()
	}()

	err := p.Run(func(body io.ReadCloser) error {
		defer body.Close()
		buff := make([]byte, 10)
		if _, err := body.Read(buff); err != nil {
			return fmt.Errorf("Failed to read body: %s", err)
		}
		fmt.Println(fmt.Sprintf("First 10 bytes: %s", string(buff)))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
