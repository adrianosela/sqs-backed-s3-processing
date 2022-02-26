package processor

import (
	"context"
	"fmt"
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

	sqsQueueArn string
	sqsClient   *sqs.SQS
	s3Client    *s3.S3

	workers         uint8
	batchSize       uint8
	shutdownTimeout time.Duration
}

func New(sess *session.Session, sqsQueueArn string, workers, batchSize uint8) *Processor {
	ctx, cancel := context.WithCancel(context.Background())

	return &Processor{
		ctx:         ctx,
		cancel:      cancel,
		sem:         semaphore.NewWeighted(int64(workers)),
		sqsQueueArn: sqsQueueArn,
		sqsClient:   sqs.New(sess),
		s3Client:    s3.New(sess),

		workers:         workers,
		batchSize:       batchSize,
		shutdownTimeout: time.Second * 10,
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

	// FIXME: do work - for now a random sleep
	// - get batch of SQS messages
	// - for each message, get the corresponding s3 bucket object
	// - run the given processing function (fn) for the object
	jobId := rand.Intn(1000000000)
	log.Printf("[processor][job=%d] worker starting!", jobId)
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(5000)))
	log.Printf("[processor][job=%d] worker done!", jobId)
}
