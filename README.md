
# lq
lightweight queue runner package inspired by Laravel queue for go.
since using gorm it supports all databases gorm supports. (Mysql, PostgreSQL, SQLite, SQL Server and TiDB) (https://gorm.io/docs/connecting_to_the_database.html)
the mechanism for loading queues is pulling from database.

## Features
- Support Mysql, PostgreSQL, Sqlite, SQL Server and TiDB since based on gorm
- Support multiple workers
- Graceful shutdown

## Sample Implementation
```go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	lq "github.com/oneapplab/lq"
)

// main queue cmd which works similar to laravel queue runner, listen on database
func main() {
	root, _ := os.Getwd()
	path := filepath.Join(root, "storage", "jobs.db") // get sqlite path from command line -sqlite, default '../storage/jobs.db'
	workerPtr := flag.Uint("worker", 5, "an uint8")   // get workers count from command line -worker, default 5
	flag.Parse()

	// get syscall's related events for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	defer stop()

	config := lq.Init(nil, path, 5, 10)
	job := (&lq.Job{}).Init(config)

	// Define how to process jobs
	jobProcessor := lq.NewJobProcessor(
		&ctx,
		GenericHandler,    // handler
		"default",         // queue
		uint8(*workerPtr), // 5 workers
		config,            // you can pass *gorm.DB or just path file you want sqlite database being stored
	)

	// to re-run failed jobs with max attempts 5 (each failed jobs with attempts less than 5 would run again)
	job.DispatchFailedJobs("default", 5)

	// sample:
	// Add some jobs to the queue
	for i := 0; i < 100; i++ {
		_, err := job.Create("default", "test-action", fmt.Sprintf(`{"task":"send_another_email","user_id":%d}`, i+1), time.Now())

		if err != nil {
			fmt.Printf("Failed to add job: %v\n", err)
		}
	}

	// Start processing jobs in the background
	jobProcessor.Start()
	<-ctx.Done()
	jobProcessor.Stop()
}

// GenericHandler passed to job processor to decide which method should handle related job
func GenericHandler(job lq.JobModel, workerID uint8) error {
	switch job.Handler {
	case "test-handler":
		return TestHandler(job, workerID)
	default:
		return TestHandler(job, workerID)
	}
}

func TestHandler(job lq.JobModel, workerID uint8) error {
	time.Sleep(3 * time.Second)
	log.Printf("--------- Test Handled: ---- %s ----- worker ID: %d", job.ID.String(), workerID)

	return nil
}
```