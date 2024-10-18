package lq

import (
	"context"
	"database/sql"
	"log"
	"slices"
	"sync"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// JobProcessor holds the configuration for processing jobs with a worker pool
type JobProcessor struct {
	ctx        context.Context                          // ctx to handle graceful shutdown
	DB         *gorm.DB                                 // database instance
	Handler    func(job JobModel, workerId uint8) error // Function to process each job
	Queue      string                                   // The queue to process
	Workers    uint8                                    // Number of concurrent workers
	jobChannel chan JobModel                            // Channel for job dispatching
	wg         sync.WaitGroup                           // WaitGroup to waits for all of goroutine workers to finish
	config     *Config                                  // Config JobProcessor
}

// NewJobProcessor creates a new instance of the job processor
func NewJobProcessor(
	ctx *context.Context,
	handler func(job JobModel, workerId uint8) error,
	queue string,
	workers uint8,
	config *Config,
) *JobProcessor {
	return &JobProcessor{
		ctx:        *ctx,
		DB:         config.db,
		Handler:    handler,
		Queue:      queue,
		Workers:    workers,
		jobChannel: make(chan JobModel, workers),
		config:     config,
	}
}

// Start the worker pool and begins processing jobs concurrently
func (p *JobProcessor) Start() {
	log.Printf("Starting %d workers...", p.Workers)

	// keep all current jobs being handled by each worker
	workerJobs := make([]string, p.Workers)

	// Spawn workers
	for i := range p.Workers {
		p.wg.Add(1)
		go p.worker(i, workerJobs)
	}

	// Monitor for new jobs and dispatch them to the workers
	go p.dispatchJobs()

	// Wait for all workers to finish
	p.wg.Wait()
}

// worker is a goroutine that processes jobs from the jobChannel
func (p *JobProcessor) worker(workerID uint8, workerJobs []string) {
	defer p.wg.Done()

	for job := range p.jobChannel {
		// check index in workerJobs to make sure 2 workers won't work on same job (idempotent)
		inx := slices.Index(workerJobs, job.ID.String())

		// if inx isn't -1 it means another worker already picked this job and working on it
		if inx != -1 {
			log.Printf("Worker %d: Job ID: %s already processing by worker id %d", workerID, job.ID.String(), inx)
			continue
		}

		workerJobs[workerID] = job.ID.String()

		res := p.DB.First(&job)

		if res.Error != nil {
			log.Printf("Worker %d: failed to select job ID: %s, Error: %v", workerID, job.ID.String(), res.Error.Error())

			continue
		}

		log.Printf("Worker %d: processing job ID: %s Attempts: %d ", workerID, job.ID.String(), job.Attempts)

		// Process the job
		err := p.Handler(job, workerID)
		if err != nil {
			log.Printf("Worker %d: failed to process job ID: %s, Error: %v", workerID, job.ID.String(), err)
			p.markJobAsFailed(&job, err)
		} else {
			p.markJobAsCompleted(&job)
		}
	}
}

// dispatchJobs continuously monitors for new jobs and dispatches them to workers
func (p *JobProcessor) dispatchJobs() {
	for {
		var job JobModel
		tx := p.DB.Begin()
		// Find the next available job in the queue
		// SKIP LOCKED would skip all rows that are being proceed by other transactions
		result := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("queue = ? AND available_at <= ? AND reserved_at IS NULL AND failed_at IS NULL",
				p.Queue,
				time.Now().UnixMilli(),
			).
			First(&job)

		if result.Error != nil {
			tx.Commit()
			log.Printf("Error fetching job: %v", result.Error)
			time.Sleep(time.Duration(p.config.sleep) * time.Second)
		} else if result.RowsAffected != 0 {
			// Mark job as processing by setting reserved_at and increase attempts count
			job.ReservedAt = sql.NullInt64{
				Valid: true,
				Int64: time.Now().UnixMilli(),
			}
			job.Attempts = job.Attempts + 1
			tx.Save(&job)
			tx.Commit()
			// Dispatch job to the jobChannel for workers to process
			p.jobChannel <- job
			time.Sleep(time.Duration(p.config.interval) * time.Millisecond)
		} else {
			tx.Commit()
			log.Printf("No jobs found")
			time.Sleep(time.Duration(p.config.sleep) * time.Second) // No job found, wait before checking again
		}

		select {
		case <-p.ctx.Done(): // if cancel() execute
			close(p.jobChannel)
			return
		default:
			continue
		}
	}
}

// markJobAsCompleted marks the job as completed by deleting them from table
func (p *JobProcessor) markJobAsCompleted(job *JobModel) {
	p.DB.Delete(&job)
}

// markJobAsFailed marks the job as failed by setting the failed_at timestamp and error message
func (p *JobProcessor) markJobAsFailed(job *JobModel, err error) {
	job.FailedAt = sql.NullInt64{
		Valid: true,
		Int64: time.Now().UnixMilli(),
	}
	job.ReservedAt = sql.NullInt64{
		Valid: false,
	}
	job.Error = err.Error()
	p.DB.Save(&job)
	log.Printf("Job ID: %s has failed", job.ID.String())
}

// Stop stops the job processor by closing the job channel and waiting for all workers to finish
func (p *JobProcessor) Stop() {
	_, ok := (<-p.jobChannel)
	if ok { // If we can receive on the channel then it is NOT closed
		close(p.jobChannel) // Close the job channel to stop workers
	}
	p.wg.Wait() // Wait for all workers to finish processing
	log.Println("All workers stopped")
}
