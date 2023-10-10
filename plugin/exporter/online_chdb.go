package exporter

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

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

func (oe *onlineExporter) exportStake() error {
	batch, err := oe.chdb.PrepareBatch(oe.ctx, "INSERT INTO online_stake")
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
