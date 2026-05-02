package model

// FieldDef defines a single field in a pipeline schema.
type FieldDef struct {
	Name     string `yaml:"name"`     // Field name in output
	Type     string `yaml:"type"`     // ClickHouse type (String, UInt32, IPv4, DateTime, etc.)
	Nullable bool   `yaml:"nullable"` // Whether the field allows null
	Comment  string `yaml:"comment"`  // Field description
}

// Schema defines the full schema for a pipeline.
type Schema struct {
	Fields []FieldDef `yaml:"fields"`
}

// FieldNames returns all field names in order.
func (s *Schema) FieldNames() []string {
	names := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		names[i] = f.Name
	}
	return names
}
