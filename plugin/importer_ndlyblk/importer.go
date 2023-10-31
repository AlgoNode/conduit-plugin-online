package importer_ndlyblk

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/algorand/go-algorand-sdk/v2/types"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"

	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/plugins"
	"github.com/algorand/conduit/conduit/plugins/importers"
)

//go:embed sample.yaml
var sampleConfig string

// metadata contains information about the plugin used for CLI helpers.
var metadata = plugins.Metadata{
	Name:         "ndly_blksrv",
	Description:  "Nodely Block Server importer",
	Deprecated:   false,
	SampleConfig: sampleConfig,
}

func init() {
	importers.Register(metadata.Name, importers.ImporterConstructorFunc(func() importers.Importer {
		return &iBS{}
	}))
}

type BlkSrvConfig struct {
	Url   string `yaml:"url"`
	Token string `yaml:"token"`
}

type Config struct {
	BlkSrv BlkSrvConfig `yaml:"blksrv"`
}

// iBS is the object which implements the importer plugin interface.
type iBS struct {
	log *logrus.Logger
	cfg Config
	hc  *http.Client
	ctx context.Context
}

func (it *iBS) Metadata() plugins.Metadata {
	return metadata
}

func (it *iBS) Config() string {
	ret, _ := yaml.Marshal(it.cfg)
	return string(ret)
}

func (it *iBS) Close() error {
	return nil
}

func (it *iBS) Init(ctx context.Context, _ data.InitProvider, cfg plugins.PluginConfig, logger *logrus.Logger) error {
	it.log = logger
	it.ctx = ctx
	if err := cfg.UnmarshalConfig(&it.cfg); err != nil {
		return fmt.Errorf("unable to read configuration: %w", err)
	}

	ht := http.DefaultTransport.(*http.Transport).Clone()
	ht.MaxConnsPerHost = 100
	ht.MaxIdleConns = 100
	ht.MaxIdleConnsPerHost = 100

	it.hc = &http.Client{
		Timeout:   time.Second * 5,
		Transport: ht,
	}

	return nil
}

func (it *iBS) GetGenesis() (*types.Genesis, error) {
	url := fmt.Sprintf("%s/n2/conduit/genesis", it.cfg.BlkSrv.Url)
	req, err := http.NewRequestWithContext(it.ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/msgpack")
	resp, err := it.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	blob, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return getGenesisFromGenesisBlob(blob)
}

func (it *iBS) GetBlock(rnd uint64) (data.BlockData, error) {
	url := fmt.Sprintf("%s/n2/conduit/blockdata/%d", it.cfg.BlkSrv.Url, rnd)
	req, err := http.NewRequestWithContext(it.ctx, http.MethodGet, url, nil)
	if err != nil {
		return data.BlockData{}, err
	}
	req.Header.Set("Content-Type", "application/msgpack")
	resp, err := it.hc.Do(req)
	if err != nil {
		return data.BlockData{}, err
	}
	defer resp.Body.Close()
	blob, err := io.ReadAll(resp.Body)
	if err != nil {
		return data.BlockData{}, err
	}
	bd, err := getBlockDataFromBDBlob(blob)
	if err != nil {
		return data.BlockData{}, err
	}
	return *bd, nil
}
