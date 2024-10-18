package lq

import (
	"errors"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Config struct {
	db       *gorm.DB // gorm instance
	sleep    uint8    // sleep duration when there is no job in table or fail to get results from database (used to moderates system resources) in seconds
	interval uint16   // sleep after fetching each job in millisecond
}

var configInstance *Config

func Init(db *gorm.DB, sqliteDBPath string, sleep uint8, interval uint16) *Config {
	if db == nil {
		// Initialize SQLite database
		database, err := gorm.Open(sqlite.Open(sqliteDBPath), &gorm.Config{})
		if err != nil {
			panic("failed to connect to the database")
		}

		db = database
	}

	configInstance = &Config{
		db:       db,
		sleep:    sleep,
		interval: interval,
	}

	return configInstance
}

func GetConfig() (*Config, error) {
	if configInstance == nil {
		return nil, errors.New("config not found. call Init method first")
	}

	return configInstance, nil
}
