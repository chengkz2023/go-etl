package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go-etl/model"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketFileStatus = []byte("file_status")
)

// FileStore persists file processing status using bolt.
type FileStore struct {
	db       *bolt.DB
	dbPath   string
}

// NewFileStore opens or creates a bolt database for file status tracking.
func NewFileStore(dbPath string) (*FileStore, error) {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open bolt DB: %w", err)
	}

	// Create bucket if not exists
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketFileStatus)
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("create bucket: %w", err)
	}

	return &FileStore{db: db, dbPath: dbPath}, nil
}

// key returns the bolt key for a pipeline + file path combination.
func key(pipelineName, filePath string) []byte {
	return []byte(pipelineName + ":" + filePath)
}

// GetStatus returns the file's processing status, or StatusUnknown if not tracked.
func (s *FileStore) GetStatus(pipelineName, filePath string) (model.FileStatus, error) {
	var status model.FileStatus
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFileStatus)
		data := b.Get(key(pipelineName, filePath))
		if data == nil {
			status = model.StatusUnknown
			return nil
		}
		var rec model.FileRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			return err
		}
		status = rec.Status
		return nil
	})
	return status, err
}

// GetRecord returns the full file record.
func (s *FileStore) GetRecord(pipelineName, filePath string) (*model.FileRecord, error) {
	var rec model.FileRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFileStatus)
		data := b.Get(key(pipelineName, filePath))
		if data == nil {
			return nil
		}
		return json.Unmarshal(data, &rec)
	})
	if err != nil {
		return nil, err
	}
	if rec.FilePath == "" {
		return nil, nil
	}
	return &rec, nil
}

// SetPending marks a file as pending.
func (s *FileStore) SetPending(pipelineName, filePath string, size int64, modTime time.Time) error {
	return s.save(&model.FileRecord{
		PipelineName: pipelineName,
		FilePath:     filePath,
		FileSize:     size,
		FileModTime:  modTime,
		Status:       model.StatusPending,
		ProcessedAt:  time.Now(),
	})
}

// SetProcessing marks a file as currently being processed.
func (s *FileStore) SetProcessing(pipelineName, filePath string) error {
	return s.updateStatus(pipelineName, filePath, model.StatusProcessing)
}

// SetDone marks a file as successfully processed.
func (s *FileStore) SetDone(pipelineName, filePath string, rowCount int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFileStatus)
		k := key(pipelineName, filePath)
		data := b.Get(k)
		if data == nil {
			return nil
		}
		var rec model.FileRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			return err
		}
		rec.Status = model.StatusDone
		rec.Rows = rowCount
		rec.ProcessedAt = time.Now()
		rec.Error = ""

		newData, err := json.Marshal(&rec)
		if err != nil {
			return err
		}
		return b.Put(k, newData)
	})
}

// SetFailed marks a file as failed with an error message.
func (s *FileStore) SetFailed(pipelineName, filePath string, errMsg string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFileStatus)
		k := key(pipelineName, filePath)
		data := b.Get(k)
		if data == nil {
			return nil
		}
		var rec model.FileRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			return err
		}
		rec.Status = model.StatusFailed
		rec.ProcessedAt = time.Now()
		rec.Error = errMsg

		newData, err := json.Marshal(&rec)
		if err != nil {
			return err
		}
		return b.Put(k, newData)
	})
}

// ListPending returns all files with pending status for a pipeline.
func (s *FileStore) ListPending(pipelineName string) ([]*model.FileRecord, error) {
	var records []*model.FileRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFileStatus)
		c := b.Cursor()
		prefix := []byte(pipelineName + ":")
		for k, v := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, v = c.Next() {
			var rec model.FileRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			if rec.Status == model.StatusPending {
				records = append(records, &rec)
			}
		}
		return nil
	})
	return records, err
}

// IsNewFile checks if a file has never been seen before (unknown status).
func (s *FileStore) IsNewFile(pipelineName, filePath string) (bool, error) {
	status, err := s.GetStatus(pipelineName, filePath)
	if err != nil {
		return false, err
	}
	return status == model.StatusUnknown, nil
}

func (s *FileStore) save(rec *model.FileRecord) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFileStatus)
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		return b.Put(key(rec.PipelineName, rec.FilePath), data)
	})
}

func (s *FileStore) updateStatus(pipelineName, filePath string, status model.FileStatus) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFileStatus)
		k := key(pipelineName, filePath)
		data := b.Get(k)
		if data == nil {
			return nil
		}
		var rec model.FileRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			return err
		}
		rec.Status = status
		rec.ProcessedAt = time.Now()

		newData, err := json.Marshal(&rec)
		if err != nil {
			return err
		}
		return b.Put(k, newData)
	})
}

// Close closes the bolt database.
func (s *FileStore) Close() error {
	return s.db.Close()
}
