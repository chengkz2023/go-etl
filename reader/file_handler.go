package reader

import (
	"os"

	"go-etl/model"
)

// FileHandler reads and parses a single file into Row batches.
type FileHandler struct {
	reader    *Reader
	filePath  string
	batchSize int
}

// NewFileHandler creates a handler for one file.
func NewFileHandler(r *Reader, filePath string, batchSize int) *FileHandler {
	return &FileHandler{
		reader:    r,
		filePath:  filePath,
		batchSize: batchSize,
	}
}

// ReadBatches opens the file and returns rows in batches via channel.
func (h *FileHandler) ReadBatches() (<-chan []model.Row, <-chan error) {
	rowCh := make(chan []model.Row, 2)
	errCh := make(chan error, 1)

	go func() {
		defer close(rowCh)
		defer close(errCh)

		f, err := os.Open(h.filePath)
		if err != nil {
			errCh <- err
			return
		}
		defer f.Close()

		allRows, err := h.reader.ReadAll(f)
		if err != nil {
			errCh <- err
			return
		}

		for i := 0; i < len(allRows); i += h.batchSize {
			end := i + h.batchSize
			if end > len(allRows) {
				end = len(allRows)
			}
			rowCh <- allRows[i:end]
		}
	}()

	return rowCh, errCh
}
