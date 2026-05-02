package transform

import "go-etl/model"

// HeaderMerger merges a static header meta row into every row.
// This is the programmatic version — the reader already does this inline.
// It's useful when the header meta is known at pipeline build time.
type HeaderMerger struct {
	Meta model.Row
}

// NewHeaderMerger creates a header merger transformer.
func NewHeaderMerger(meta model.Row) *HeaderMerger {
	return &HeaderMerger{Meta: meta}
}

// Name returns the transformer name.
func (m *HeaderMerger) Name() string { return "header_merger" }

// Transform merges the static meta fields into the row.
// Existing row fields take precedence over meta fields.
func (m *HeaderMerger) Transform(row model.Row) (model.Row, error) {
	for k, v := range m.Meta {
		if _, exists := row[k]; !exists {
			row[k] = v
		}
	}
	return row, nil
}
