package model

// Row represents a single data row as field name → value mapping.
type Row map[string]string

// Clone returns a deep copy of the row.
func (r Row) Clone() Row {
	c := make(Row, len(r))
	for k, v := range r {
		c[k] = v
	}
	return c
}

// Batch represents a collection of rows for batch processing.
type Batch struct {
	Rows          []Row
	PipelineName  string
	FileName      string
}
