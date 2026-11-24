package exporter_onlch

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sirupsen/logrus"
)

// aggRows holds aggregate rows waiting to be inserted, column by column.
type aggRows struct {
	addr   []string
	rnd    []uint64
	ts     []int64
	rndOnl []int32
	sfsum  []float64
}

func (r *aggRows) empty() bool { return len(r.addr) == 0 }

// AggregateBundle collects aggregate rows of several bins while catching up and
// inserts them as one batch. Rows are kept in memory: the INSERT is only opened
// (PrepareBatch) right before it is sent, so no INSERT stays open on the server
// between bins (an open one showed up as 23-92 s inserts in query_log).
type AggregateBundle struct {
	batchLimit int
	batches    int
	rows       aggRows
	insStr     string
	chdb       clickhouse.Conn
	ctx        context.Context
	lastRound  uint64
	lastTs     time.Time
	isCatchup  bool
	log        *logrus.Logger
	sendFn     func(rows *aggRows) error // nil = sendCH; tests replace it
}

func (oe *onlineExporter) MakeBatcher() *AggregateBundle {
	ab := &AggregateBundle{
		batchLimit: 0,
		batches:    0,
		insStr:     "INSERT INTO " + oe.cfg.ChAggTab,
		chdb:       oe.chdb,
		ctx:        oe.ctx,
		lastRound:  0,
		lastTs:     time.Now(),
		isCatchup:  false,
		log:        oe.log,
	}
	// Enable bundle of batches
	if oe.cfg.ChAggBatch {
		ab.batchLimit = 100
	}
	return ab
}

func (ab *AggregateBundle) Monitor(round uint64) bool {
	now := time.Now()
	tDelta := now.Sub(ab.lastTs)
	ab.isCatchup = round-ab.lastRound == 1 && tDelta < time.Second
	ab.lastTs = now
	ab.lastRound = round
	return ab.isCatchup
}

// Add queues the rows of one aggregate bin.
func (ab *AggregateBundle) Add(addr []string, rnd []uint64, ts []int64, rndOnl []int32, sfsum []float64) {
	ab.rows.addr = append(ab.rows.addr, addr...)
	ab.rows.rnd = append(ab.rows.rnd, rnd...)
	ab.rows.ts = append(ab.rows.ts, ts...)
	ab.rows.rndOnl = append(ab.rows.rndOnl, rndOnl...)
	ab.rows.sfsum = append(ab.rows.sfsum, sfsum...)
}

// SmartFlush counts one more bin in the bundle and sends the bundle unless we
// are catching up and the bundle is still below its limit.
func (ab *AggregateBundle) SmartFlush() error {
	if ab.rows.empty() {
		return nil
	}
	ab.batches++
	if !ab.isCatchup || ab.batches > ab.batchLimit {
		return ab.Flush()
	}
	return nil
}

// Flush sends whatever is queued (used by SmartFlush and on Close).
func (ab *AggregateBundle) Flush() error {
	if ab.rows.empty() {
		return nil
	}
	if ab.batches > 1 {
		ab.log.Infof("Flushing bundle of %d batches", ab.batches)
	}
	send := ab.sendFn
	if send == nil {
		send = ab.sendCH
	}
	err := send(&ab.rows)
	ab.rows = aggRows{}
	ab.batches = 0
	return err
}

func (ab *AggregateBundle) sendCH(r *aggRows) error {
	batch, err := ab.chdb.PrepareBatch(ab.ctx, ab.insStr)
	if err != nil {
		return err
	}
	if err := batch.Column(0).Append(r.addr); err != nil {
		return err
	}
	if err := batch.Column(1).Append(r.rnd); err != nil {
		return err
	}
	if err := batch.Column(2).Append(r.ts); err != nil {
		return err
	}
	if err := batch.Column(3).Append(r.rndOnl); err != nil {
		return err
	}
	if err := batch.Column(4).Append(r.sfsum); err != nil {
		return err
	}
	return batch.Send()
}
