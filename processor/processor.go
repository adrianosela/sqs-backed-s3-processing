package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"golang.org/x/sync/semaphore"
)

type Processor struct {
	ctx    context.Context
	cancel context.CancelFunc

	sem *semaphore.Weighted

	sqsQueueURL string
	sqsClient   *sqs.SQS
	s3Client    *s3.S3

	deleteMessagesAfter bool

	workers            uint8
	batchSize          uint8
	shutdownTimeout    time.Duration
	failureBackoffTime time.Duration
}

// there doesn't seem to be a go-sdk built-in
// definition for the sqs message body of an
// s3 delivery notification, so we roll our own
type msgBody struct {
	Records []struct {
		S3 struct {
			Bucket struct {
				Name string `json:"name"`
			} `json:"bucket"`
			Object struct {
				Key string `json:"key"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

func New(sess *session.Session, sqsQueueURL string, workers, batchSize uint8, deleteAfter bool) *Processor {
	ctx, cancel := context.WithCancel(context.Background())

	return &Processor{
		ctx:         ctx,
		cancel:      cancel,
		sem:         semaphore.NewWeighted(int64(workers)),
		sqsQueueURL: sqsQueueURL,
		sqsClient:   sqs.New(sess),
		s3Client:    s3.New(sess),

		workers:            workers,
		batchSize:          batchSize,
		shutdownTimeout:    time.Second * 10,
		failureBackoffTime: time.Second * 1,

		deleteMessagesAfter: deleteAfter,
	}
}

func (p *Processor) Run(fn func(io.ReadCloser) error) error {

	// stop the processor on os interrupt signals (e.g. SIGINT, SIGTERM...)
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)
	go func() {
		signal := <-sigc
		log.Printf("[processor] Signal \"%s\" received...", signal.String())
		p.Stop()
	}()

	for {
		select {
		case <-p.ctx.Done():
			log.Printf("[processor] Stopping processor gracefully...")

			// max wait for any workers to finish current work
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), p.shutdownTimeout)
			defer shutdownCancel()

			// block until all resources are available i.e. all workers are done
			if err := p.sem.Acquire(shutdownCtx, int64(p.workers)); err != nil {
				if err == shutdownCtx.Err() {
					log.Println("[processor] Processor graceful shutdown exceeded timeout, exiting.")
					return fmt.Errorf("Processor did not shutdown gracefully within the set timeout")
				} else {
					log.Printf("[processor] Processor graceful shutdown failed to acquire resources: %s")
					return fmt.Errorf("Processor could not be stopped gracefully, an unknown error occurred: %s")
				}
			}
			defer p.sem.Release(int64(p.workers))

			log.Println("[processor] Processor stopped gracefully!")
			return nil
		default:
			if p.sem.TryAcquire(1) {
				go p.doBatch(fn)
			}
		}
	}

	return nil
}

func (p *Processor) Stop() {
	p.cancel()
}

func (p *Processor) doBatch(fn func(io.ReadCloser) error) {
	defer p.sem.Release(1)

	jobId := rand.Intn(1000000000)
	log.Printf("[processor] [job=%d] worker starting!", jobId)

	var err error
	defer func() {
		if err != nil {
			// if an error was encounterd during doBatch
			// we sleep for some time before releasing the worker
			// to avoid the next worker running into the same scenario.
			time.Sleep(p.failureBackoffTime)
		}
	}()

	receiveMessageOutput, err := p.sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(p.sqsQueueURL),
		MaxNumberOfMessages: aws.Int64(int64(p.batchSize)),
	})
	if err != nil {
		log.Printf("[processor] [job=%d] worker failed to receive messages: %s", jobId, err)
		return // handle?
	}

	if receiveMessageOutput.Messages == nil || len(receiveMessageOutput.Messages) < 1 {
		log.Printf("[processor] [job=%d] worker found no work!", jobId)
		return // handle?
	}

	for _, msg := range receiveMessageOutput.Messages {
		var body msgBody
		if err = json.Unmarshal([]byte(aws.StringValue(msg.Body)), &body); err != nil {
			log.Printf("[processor] [job=%d] worker failed to json decode message body: %s", jobId, err)
			return // handle?
		}
		if body.Records == nil || len(body.Records) < 1 {
			log.Printf("[processor] [job=%d] worker found no work!: %s", jobId, err)
			return // handle?
		}
		bucket := body.Records[0].S3.Bucket.Name
		object := body.Records[0].S3.Object.Key
		getObjectOutput, err := p.s3Client.GetObject(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(object)})
		if err != nil {
			log.Printf("[processor] [job=%d] worker failed to retrieve %s/%s from S3: %s", jobId, bucket, object, err)
			return // handle?
		}
		if err = fn(getObjectOutput.Body); err != nil {
			log.Printf("[processor] [job=%d] object processing function returned an error for %s/%s: %s", jobId, bucket, object, err)
			return // handle?
		}
		if p.deleteMessagesAfter {
			if _, err := p.sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      aws.String(p.sqsQueueURL),
				ReceiptHandle: msg.ReceiptHandle,
			}); err != nil {
				log.Printf("[processor] [job=%d] Unable to delete message %s after processing: %s", jobId, aws.StringValue(msg.ReceiptHandle), err)
				return // handle?
			}
		}
	}

	log.Printf("[processor] [job=%d] worker done!", jobId)
}
