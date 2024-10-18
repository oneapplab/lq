package lq

import (
	"database/sql"
	"log"
	"time"

	"github.com/google/uuid"
)

type Job struct {
	config *Config
}

type JobModel struct {
	ID          uuid.UUID `gorm:"primaryKey;type:uuid;"`
	Queue       string    `gorm:"index,default:'default'"`
	Handler     string
	Payload     string `gorm:"type:longtext"`
	Error       string `gorm:"type:longtext"`
	Attempts    uint8  `gorm:"default:0"`
	ReservedAt  sql.NullInt64
	FailedAt    sql.NullInt64
	AvailableAt int64
	CreatedAt   int64
}

func (JobModel) TableName() string {
	return "jobs"
}

func (j *Job) Init(config *Config) *Job {
	// InitializeJobModel creates the jobs table if it doesn't exist
	err := config.db.AutoMigrate(&JobModel{})

	if err != nil {
		panic(err.Error())
	}

	j.config = config

	return j
}

// Create creates a new job in the database
func (j *Job) Create(
	queue string, // queue - should be same as processor has
	handler string, // handler - it helps to decide which method should handle this specific job
	payload string, // payload - (usually json) the payload data handler needs to handle the job
	available time.Time, // available time - should be used to delay running job or just pass time.Now() to ignore delay
) (*JobModel, error) {
	job := &JobModel{
		ID:          uuid.New(),
		Queue:       queue,
		Handler:     handler,
		Payload:     payload,
		AvailableAt: available.UnixMilli(),
		CreatedAt:   time.Now().UnixMilli(),
	}

	if err := j.config.db.Create(job).Error; err != nil {
		return nil, err
	}

	return job, nil
}

// DeleteOlderJobs deletes messages that are older than passed date
func (j *Job) DeleteOlderJobs(
	date time.Time, // example: time.Date(2024, 04, 4, 0, 0, 0, 0, nil)
) {
	j.config.db.Exec("delete from jobs where created_at < ?", date.UnixMilli())
}

// DispatchFailedJobs update failed jobs to re run again by workers
func (j *Job) DispatchFailedJobs(
	queue string,
	maxAttempts int,
) {
	if maxAttempts == 0 {
		res := j.config.db.Exec("update jobs set failed_at=null where failed_at is not null and queue = ?", queue)

		if res.Error != nil {
			log.Panic(res.Error.Error())
		}
	} else {
		j.config.db.
			Exec(
				"update jobs set failed_at=null where failed_at is not null and queue = ? and attempts <= ?",
				queue,
				maxAttempts,
			)
	}
}
