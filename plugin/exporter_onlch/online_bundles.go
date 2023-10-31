package exporter_onlch

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/sirupsen/logrus"
)

type AggregateBundle struct {
	batchLimit int
	batches    int
	batch      driver.Batch
	insStr     string
	chdb       clickhouse.Conn
	ctx        context.Context
	lastRound  uint64
	lastTs     time.Time
	isCatchup  bool
	log        *logrus.Logger
}

func (oe *onlineExporter) MakeBatcher() *AggregateBundle {
	ab := &AggregateBundle{
		batchLimit: 0,
		batches:    0,
		batch:      nil,
		insStr:     fmt.Sprintf("INSERT INTO " + oe.cfg.ChAggTab),
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

func (ab *AggregateBundle) GetBatch() (driver.Batch, error) {
	if ab.batch == nil {
		batch, err := ab.chdb.PrepareBatch(ab.ctx, ab.insStr)
		if err != nil {
			return nil, err
		}
		ab.batch = batch
	}
	return ab.batch, nil
}

func (ab *AggregateBundle) SmartFlush() error {
	if ab.batch == nil {
		return nil
	}
	ab.batches++
	if !ab.isCatchup || ab.batches > ab.batchLimit {
		if ab.batches > 1 {
			ab.log.Infof("Flushing bundle of %d batches", ab.batches)
		}
		err := ab.batch.Send()
		ab.batch = nil
		ab.batches = 0
		return err
	}
	return nil
}
