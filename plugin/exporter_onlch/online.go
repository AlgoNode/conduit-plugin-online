package exporter_onlch

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/plugins"
	"github.com/algorand/conduit/conduit/plugins/exporters"
	"github.com/algorand/go-algorand-sdk/v2/types"
)

//go:embed sample.yaml
var sampleConfig string

// metadata contains information about the plugin used for CLI helpers.
var metadata = plugins.Metadata{
	Name:         "online_clickhouse",
	Description:  "Exports participation account state aggregates to ClickHouse.",
	Deprecated:   false,
	SampleConfig: sampleConfig,
}

func init() {
	exporters.Register(metadata.Name, exporters.ExporterConstructorFunc(func() exporters.Exporter {
		return &onlineExporter{}
	}))
}

// onlineExporter is the object which implements the exporter plugin interface.
type onlineExporter struct {
	log     *logrus.Logger
	cfg     Config
	ctx     context.Context
	onls    *onlineStakeState
	batcher *AggregateBundle
	chdb    clickhouse.Conn
}

func (oe *onlineExporter) Metadata() plugins.Metadata {
	return metadata
}

func (oe *onlineExporter) Config() string {
	ret, _ := yaml.Marshal(oe.cfg)
	return string(ret)
}

func (oe *onlineExporter) Close() error {
	oe.log.Infof("Shutting down")
	oe.batcher.SmartFlush()
	return nil
}

// persistOnlineStakeState persists current online state in JSON file
func (oe *onlineExporter) persistOnlineStakeState() error {
	jPayload, err := json.MarshalIndent(oe.onls, "", " ")
	if err != nil {
		return err
	}
	fName := filepath.Join(oe.cfg.datadir, oe.cfg.StateFile)
	err = os.WriteFile(fName, jPayload, 0644)
	return err
}

// loadOnlineStakeState loads persisted state from JSON file or Genesis if starting from round 0
func (oe *onlineExporter) loadOnlineStakeState(ip data.InitProvider) (*onlineStakeState, error) {
	oe.log.Infof("Loading stake at round %d", ip.NextDBRound())
	onls := &onlineStakeState{
		Accounts:     make(map[types.Address]*partAccount),
		TotalStake:   0,
		UpdatedAtRnd: 0,
		NextExpiry:   math.MaxInt64,
		dirty:        true,
		log:          oe.log,
		ip:           ip,
	}
	if ip.NextDBRound() == 0 {
		onls.loadFromGenesis()
		onls.updateTotals(0)
		// TODO: Genesis state is invalid (empty) for the first 320 rounds
		return onls, nil
	}
	//BUG: refuse to sync where NextDBRound > Clickhouse batch

	fName := filepath.Join(oe.cfg.datadir, oe.cfg.StateFile)
	file, err := os.Open(fName)
	if err != nil {
		oe.log.Errorf("Error opening file: %v", err)
		return nil, err
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		oe.log.Errorf("Error reading file: %v", err)
		return nil, err
	}
	err = json.Unmarshal(content, onls)
	if err != nil {
		oe.log.Errorf("Error reading state: %v", err)
		return nil, err
	}

	if ip.NextDBRound() < onls.UpdatedAtRnd {
		err := fmt.Errorf("state round %d after nextDBRound", onls.UpdatedAtRnd)
		oe.log.Errorf("Error reading state: %v", err)
		return nil, err
	}
	onls.updateTotals(ip.NextDBRound())

	return onls, nil
}

func (oe *onlineExporter) Init(ctx context.Context, ip data.InitProvider, cfg plugins.PluginConfig, logger *logrus.Logger) error {
	var err error
	oe.log = logger
	oe.ctx = ctx
	if err := cfg.UnmarshalConfig(&oe.cfg); err != nil {
		return fmt.Errorf("unable to read configuration: %w", err)
	}

	if err = oe.chdbInit(); err != nil {
		return err
	}
	oe.cfg.datadir = cfg.DataDir
	oe.onls, err = oe.loadOnlineStakeState(ip)
	oe.onls.aggBinSize = oe.cfg.ChAggBin
	oe.batcher = oe.MakeBatcher()
	if err != nil {
		return err
	}
	if err = oe.persistOnlineStakeState(); err != nil {
		return err
	}
	return nil
}

// ProcessTX_DFS does depth first search on the (inner)transaction tree for account registrations
func (oe *onlineExporter) ProcessTX_DFS(round types.Round, tx *types.SignedTxnWithAD) {
	switch tx.Txn.Type {
	case types.KeyRegistrationTx:
		oe.onls.updateAccountWithKeyreg(round, tx)
	}
	for j := range tx.EvalDelta.InnerTxns {
		oe.ProcessTX_DFS(round, &tx.EvalDelta.InnerTxns[j])
	}
}

func (oe *onlineExporter) Receive(exportData data.BlockData) error {
	round := exportData.BlockHeader.Round

	isCatchup := oe.batcher.Monitor(uint64(round))
	oe.log.Infof("Processing block %d, catching-up:%t ", round, isCatchup)

	ps := exportData.Payset
	//Look for keyregs
	for i := range ps {
		oe.ProcessTX_DFS(round, &ps[i].SignedTxnWithAD)
	}

	if exportData.Delta != nil {
		for i := range exportData.Delta.Accts.Accts {
			// Only update accounts with active voting keys
			// Offline event is handled by ProcessTX_DFS while close out is handled here
			// TODO: handle old rewards , they are ignored for now
			// TODO: handle incentive rewards , they are implemented in protocol yet
			if exportData.Delta.Accts.Accts[i].VoteLastValid >= round {
				oe.onls.updateAccountWithAcctDelta(round, &exportData.Delta.Accts.Accts[i])
			}
		}
	}

	if oe.onls.updateAggregate(round) {
		if err := oe.chdbExportAggregate(exportData.BlockHeader.TimeStamp); err != nil {
			return err
		}
		oe.onls.resetAggregate(round)
	}

	if oe.onls.updateTotals(round) || !oe.batcher.isCatchup {
		// if err := oe.chdbExportStake(); err != nil {
		// 	return errs
		// }
		if err := oe.persistOnlineStakeState(); err != nil {
			return err
		}
	}

	// exportData.Delta.Totals.Online.Money does not reflect current online stake
	//
	var ta types.MicroAlgos = 0
	tb := oe.onls.TotalStake
	if exportData.Delta != nil {
		ta = exportData.Delta.Totals.Online.Money
	}
	oe.log.WithFields(logrus.Fields{"round": round}).Infof("PluginOnlineStake:%duA Delta:%duA NextExpiryAt:%d", tb, int64(ta)-int64(tb), int64(oe.onls.NextExpiry))
	return nil
}
