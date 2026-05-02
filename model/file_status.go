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
)

// FileRecord stores the metadata of a processed file.
type FileRecord struct {
	PipelineName string     `json:"pipeline_name"`
	FilePath     string     `json:"file_path"`
	FileSize     int64      `json:"file_size"`
	FileModTime  time.Time  `json:"file_mod_time"`
	Status       FileStatus `json:"status"`
	Rows         int64      `json:"rows"`
	ProcessedAt  time.Time  `json:"processed_at"`
	Error        string     `json:"error,omitempty"`
}
