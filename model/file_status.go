package model

import "time"

// FileStatus represents the processing status of a file.
type FileStatus string

const (
	StatusUnknown    FileStatus = "unknown"
	StatusPending    FileStatus = "pending"
	StatusProcessing FileStatus = "processing"
	StatusDone       FileStatus = "done"
	StatusFailed     FileStatus = "failed"
	StatusDead       FileStatus = "dead"
)

// FileRecord stores the metadata of a processed file.
type FileRecord struct {
	PipelineName string     `json:"pipeline_name"`
	FilePath     string     `json:"file_path"`
	FileSize     int64      `json:"file_size"`
	FileModTime  time.Time  `json:"file_mod_time"`
	Status       FileStatus `json:"status"`
	Rows         int64      `json:"rows"`
	Attempts     int        `json:"attempts"`
	ProcessedAt  time.Time  `json:"processed_at"`
	NextRetryAt  time.Time  `json:"next_retry_at,omitempty"`
	DeadLetterAt time.Time  `json:"dead_letter_at,omitempty"`
	DeadLetterTo string     `json:"dead_letter_to,omitempty"`
	Error        string     `json:"error,omitempty"`
}
