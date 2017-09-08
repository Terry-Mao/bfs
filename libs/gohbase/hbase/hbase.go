package hbase

type HBaseCell struct {
	Table     string
	RowKey    string
	Family    string
	Qualifier string
	Value     string
}

func (c *HBaseCell) Valid() bool {
	return c != nil && c.Table != "" && c.Family != "" && c.Qualifier != "" && c.Value != ""
}
