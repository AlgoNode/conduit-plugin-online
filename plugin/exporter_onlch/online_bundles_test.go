package exporter_onlch

import (
	"testing"

	"github.com/sirupsen/logrus"
)

func newTestBundle(limit int) (*AggregateBundle, *[]int) {
	sent := []int{}
	ab := &AggregateBundle{batchLimit: limit, log: logrus.New()}
	ab.sendFn = func(r *aggRows) error {
		if len(r.addr) != len(r.rnd) || len(r.addr) != len(r.ts) || len(r.addr) != len(r.rndOnl) || len(r.addr) != len(r.sfsum) {
			panic("column length mismatch")
		}
		sent = append(sent, len(r.addr))
		return nil
	}
	return ab, &sent
}

func addBin(ab *AggregateBundle, n int) {
	ab.Add(make([]string, n), make([]uint64, n), make([]int64, n), make([]int32, n), make([]float64, n))
}

func TestLiveBinSentAtOnce(t *testing.T) {
	ab, sent := newTestBundle(100)
	addBin(ab, 3)
	if err := ab.SmartFlush(); err != nil {
		t.Fatal(err)
	}
	if len(*sent) != 1 || (*sent)[0] != 3 || !ab.rows.empty() {
		t.Fatalf("sent=%v pending=%d", *sent, len(ab.rows.addr))
	}
}

func TestCatchupBundlesUntilLiveOrLimit(t *testing.T) {
	ab, sent := newTestBundle(2)
	ab.isCatchup = true
	addBin(ab, 2)
	_ = ab.SmartFlush()
	addBin(ab, 2)
	_ = ab.SmartFlush()
	if len(*sent) != 0 || len(ab.rows.addr) != 4 {
		t.Fatalf("held: sent=%v pending=%d", *sent, len(ab.rows.addr))
	}
	addBin(ab, 2)
	_ = ab.SmartFlush() // third bin > limit 2
	if len(*sent) != 1 || (*sent)[0] != 6 || ab.batches != 0 {
		t.Fatalf("limit: sent=%v batches=%d", *sent, ab.batches)
	}
	addBin(ab, 1)
	_ = ab.SmartFlush()
	ab.isCatchup = false
	addBin(ab, 1)
	_ = ab.SmartFlush()
	if len(*sent) != 2 || (*sent)[1] != 2 {
		t.Fatalf("live: sent=%v", *sent)
	}
}

func TestFlushOnCloseSendsHeldRows(t *testing.T) {
	ab, sent := newTestBundle(100)
	ab.isCatchup = true
	addBin(ab, 5)
	_ = ab.SmartFlush()
	if len(*sent) != 0 {
		t.Fatal("should hold while catching up")
	}
	_ = ab.Flush()
	if len(*sent) != 1 || (*sent)[0] != 5 {
		t.Fatalf("sent=%v", *sent)
	}
	_ = ab.Flush() // nothing left: no send
	if len(*sent) != 1 {
		t.Fatalf("empty flush sent: %v", *sent)
	}
}
