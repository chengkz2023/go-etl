package transform

import "go-etl/model"

// Transformer transforms a single Row.
type Transformer interface {
	Name() string
	Transform(row model.Row) (model.Row, error)
}

// Chain is an ordered list of Transformers.
type Chain []Transformer

// Apply runs a row through all transformers in sequence.
// If any transformer returns an error, the chain stops and returns the error.
func (c Chain) Apply(row model.Row) (model.Row, error) {
	var err error
	for _, t := range c {
		row, err = t.Transform(row)
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}
