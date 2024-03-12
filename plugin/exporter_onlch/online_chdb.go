package exporter_onlch

import (
	"fmt"
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
	if oe.cfg.ChOnlTab == "" || oe.isDebugRun() {
		//skip exporting snapshots to ClickHouse
		return nil
	}
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

// chdbExportTotal exports total stake state to ClickHouse table
func (oe *onlineExporter) chdbExportTotal(ts int64) error {
	if oe.cfg.ChTotTab == "" || oe.isDebugRun() {
		//skip exporting snapshots to ClickHouse
		return nil
	}
	rnd := uint64(oe.onls.lastRnd)
	rnd -= rnd % uint64(oe.cfg.ChAggBin)
	rnd += StakeLag

	oe.log.Infof("Dumping total for round %d", rnd)

	sql := fmt.Sprintf("INSERT INTO %s (round,ts,stake) VALUES (%d,%d,%d)",
		oe.cfg.ChTotTab,
		rnd,
		ts,
		oe.onls.TotalStake,
	)
	return oe.chdb.AsyncInsert(oe.ctx, sql, false)
}

// chdbExportAggregate exports current stake aggregate to ClickHouse table
func (oe *onlineExporter) chdbExportAggregate(ts int64) error {
	if oe.cfg.ChAggTab == "" || oe.isDebugRun() {
		//skip exporting aggregates to ClickHouse
		return nil
	}
	var (
		c_addr   []string
		c_rnd    []uint64
		c_ts     []int64
		c_rndOnl []int32
		c_sfsum  []float64
	)
	batch, err := oe.batcher.GetBatch()
	if err != nil {
		return err
	}

	rnd := uint64(oe.onls.lastRnd)
	rnd -= rnd % uint64(oe.cfg.ChAggBin)
	rnd += StakeLag
	for _, acc := range oe.onls.Accounts {
		c_addr = append(c_addr, acc.Addr)
		c_rnd = append(c_rnd, rnd)
		c_ts = append(c_ts, ts)
		c_rndOnl = append(c_rndOnl, acc.AggOnline)
		c_sfsum = append(c_sfsum, acc.AggSFSum)
	}

	if err := batch.Column(0).Append(c_addr); err != nil {
		return err
	}
	if err := batch.Column(1).Append(c_rnd); err != nil {
		return err
	}
	if err := batch.Column(2).Append(c_ts); err != nil {
		return err
	}
	if err := batch.Column(3).Append(c_rndOnl); err != nil {
		return err
	}
	if err := batch.Column(4).Append(c_sfsum); err != nil {
		return err
	}
	return oe.batcher.SmartFlush()
}
