package exporter

type Config struct {
	StateFile string `yaml:"statefile"`
	ChHost    string `yaml:"clickhouse-host"`
	ChUser    string `yaml:"clickhouse-user"`
	ChPass    string `yaml:"clickhouse-pass"`
	ChDB      string `yaml:"clickhouse-db"`
	datadir   string
}
