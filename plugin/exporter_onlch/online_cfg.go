package exporter_onlch

type Config struct {
	StateFile  string `yaml:"statefile"`
	ChHost     string `yaml:"clickhouse-host"`
	ChUser     string `yaml:"clickhouse-user"`
	ChPass     string `yaml:"clickhouse-pass"`
	ChDB       string `yaml:"clickhouse-db"`
	ChOnlTab   string `yaml:"snapshot-table"`
	ChAggTab   string `yaml:"aggregate-table"`
	ChAggBin   int64  `yaml:"aggregate-bin"`
	ChAggBatch bool   `yaml:"aggregate-batch"`
	datadir    string
}
