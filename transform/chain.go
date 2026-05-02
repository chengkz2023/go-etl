package transform

import (
	"go-etl/model"
)

// BatchTransform applies the chain to all rows in a batch.
// Rows that fail transformation are logged and skipped by default.
func (c Chain) BatchTransform(rows []model.Row, onError func(row model.Row, err error)) []model.Row {
	result := make([]model.Row, 0, len(rows))
	for _, row := range rows {
		out, err := c.Apply(row)
		if err != nil {
			if onError != nil {
				onError(row, err)
			}
			continue
		}
		result = append(result, out)
	}
	return result
}
