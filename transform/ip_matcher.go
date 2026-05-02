package transform

import (
	"fmt"

	"go-etl/iputil"
	"go-etl/model"
)

// IPMatcher looks up IP fields against an IP database and adds geo fields.
type IPMatcher struct {
	IPDB        *iputil.IPDB
	Fields      []string // source IP fields to match (e.g., ["src_ip", "dst_ip"])
	LabelFields []string // output field names for the matched attributes
}

// NewIPMatcher creates a new IP matcher transformer.
func NewIPMatcher(ipdb *iputil.IPDB, fields, labelFields []string) *IPMatcher {
	return &IPMatcher{
		IPDB:        ipdb,
		Fields:      fields,
		LabelFields: labelFields,
	}
}

// Name returns the transformer name.
func (m *IPMatcher) Name() string { return "ip_matcher" }

// Transform looks up each configured IP field and attaches geo attributes.
// Attributes are added with the naming: {field}_{attrKey} (e.g., src_ip_country).
func (m *IPMatcher) Transform(row model.Row) (model.Row, error) {
	for _, field := range m.Fields {
		ip, ok := row[field]
		if !ok || ip == "" {
			continue
		}

		attrs := m.IPDB.Lookup(ip)
		if attrs == nil {
			continue
		}

		// If LabelFields are specified, use them as output prefix
		// e.g., src_geo → keys become src_geo_country, src_geo_province, etc.
		// We find the matching LabelField by same index
		fieldIdx := indexOf(m.Fields, field)
		prefix := field
		if fieldIdx >= 0 && fieldIdx < len(m.LabelFields) {
			prefix = m.LabelFields[fieldIdx]
		}

		for k, v := range attrs {
			key := fmt.Sprintf("%s_%s", prefix, k)
			row[key] = v
		}
	}
	return row, nil
}

func indexOf(slice []string, item string) int {
	for i, s := range slice {
		if s == item {
			return i
		}
	}
	return -1
}
