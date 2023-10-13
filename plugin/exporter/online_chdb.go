package exporter

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// chdbInit instantiates ClickHouse client and pings the server
func (oe *onlineExporter) chdbInit() error {
	var (
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{oe.cfg.ChHost},
			Auth: clickhouse.Auth{
				Database: oe.cfg.ChDB,
				Username: oe.cfg.ChUser,
				Password: oe.cfg.ChPass,
			},
			//Debug:           true,
			DialTimeout:     time.Second,
			MaxOpenConns:    10,
			MaxIdleConns:    5,
			ConnMaxLifetime: time.Hour,
		})
	)
	if err != nil {
		return err
	}
	err = conn.Ping(oe.ctx)
	if err != nil {
		return err
	}
	oe.chdb = conn
	return nil
}

// chdbExportStake exports whole stake state to ClickHouse table
// adds extra row with "total" account address for quick per round total online stake
func (oe *onlineExporter) chdbExportStake() error {
	batch, err := oe.chdb.PrepareBatch(oe.ctx, "INSERT INTO "+oe.cfg.ChOnlTab)
	if err != nil {
		return err
	}
	var (
		c_addr []string
		c_rnd  []uint64
		c_ma   []int64
		c_sf   []float64
	)
	rnd := uint64(oe.onls.UpdatedAtRnd) + StakeLag
	for _, acc := range oe.onls.Accounts {
		c_addr = append(c_addr, acc.Addr)
		c_rnd = append(c_rnd, rnd)
		c_ma = append(c_ma, int64(acc.Stake))
		c_sf = append(c_sf, acc.stakeFraction)
	}
	c_addr = append(c_addr, "total")
	c_rnd = append(c_rnd, rnd)
	c_ma = append(c_ma, int64(oe.onls.TotalStake))
	c_sf = append(c_sf, 1.0)

	if err := batch.Column(0).Append(c_addr); err != nil {
		return err
	}
	if err := batch.Column(1).Append(c_rnd); err != nil {
		return err
	}
	if err := batch.Column(2).Append(c_ma); err != nil {
		return err
	}
	if err := batch.Column(3).Append(c_sf); err != nil {
		return err
	}
	return batch.Send()
}

// chdbExportAggregate exports current stake aggregate to ClickHouse table
func (oe *onlineExporter) chdbExportAggregate() error {
	batch, err := oe.chdb.PrepareBatch(oe.ctx, "INSERT INTO "+oe.cfg.ChAggTab)
	if err != nil {
		return err
	}
	var (
		c_addr   []string
		c_rnd    []uint64
		c_rndOnl []int32
		c_sfsum  []float64
	)
	rnd := uint64(oe.onls.lastRnd)
	rnd -= rnd % uint64(oe.cfg.ChAggBin)
	rnd += StakeLag
	for _, acc := range oe.onls.Accounts {
		c_addr = append(c_addr, acc.Addr)
		c_rnd = append(c_rnd, rnd)
		c_rndOnl = append(c_rndOnl, acc.AggOnline)
		c_sfsum = append(c_sfsum, acc.AggSFSum)
	}

	if err := batch.Column(0).Append(c_addr); err != nil {
		return err
	}
	if err := batch.Column(1).Append(c_rnd); err != nil {
		return err
	}
	if err := batch.Column(2).Append(c_rndOnl); err != nil {
		return err
	}
	if err := batch.Column(3).Append(c_sfsum); err != nil {
		return err
	}
	return batch.Send()
}
