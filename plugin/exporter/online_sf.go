package exporter

import (
	"encoding/json"
	"math"

	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/go-algorand-sdk/v2/types"
	"github.com/sirupsen/logrus"
)

type EXPReason int64

const (
	Online EXPReason = iota
	Closed
	Offlined
	Expired
)

const (
	StakeLag = 320
)

func (s EXPReason) String() string {
	switch s {
	case Online:
		return "online"
	case Closed:
		return "closed"
	case Offlined:
		return "offlined"
	case Expired:
		return "expired"
	}
	return "unknown"
}

type partAccount struct {
	Addr          string           `json:"addr"`
	VoteLast      types.Round      `json:"votelast"`
	Stake         types.MicroAlgos `json:"stake"`
	UpdatedAtRnd  types.Round      `json:"updated"`
	AggSFSum      float64          `json:"aggsfsum"`
	AggOnline     int64            `json:"aggonlrnd"`
	stakeFraction float64
	state         EXPReason
}

type OnlineAccounts map[types.Address]*partAccount

type onlineStakeState struct {
	Accounts     OnlineAccounts   `json:"accounts"`
	TotalStake   types.MicroAlgos `json:"totalstake"`
	UpdatedAtRnd types.Round      `json:"updated"`
	NextExpiry   types.Round      `json:"nextexpiry"`
	aggBinSize   int64
	dirty        bool
	log          *logrus.Logger
	ip           data.InitProvider
}

func (i OnlineAccounts) MarshalJSON() ([]byte, error) {
	x := make(map[string]*partAccount)
	for k, v := range i {
		x[k.String()] = v
	}
	return json.Marshal(x)
}

func (i *OnlineAccounts) UnmarshalJSON(b []byte) error {
	x := make(map[string]*partAccount)
	if err := json.Unmarshal(b, &x); err != nil {
		return err
	}
	*i = make(OnlineAccounts, len(x))
	for k, v := range x {
		if ka, err := types.DecodeAddress(k); err != nil {
			return err
		} else {
			(*i)[ka] = v
		}
	}
	return nil
}

// loadFromGenesis instantiates the stake state from Genesis
func (onls *onlineStakeState) loadFromGenesis() {
	gen := onls.ip.GetGenesis()
	onls.log.Infof("Loading genesis online state")
	for _, ga := range gen.Allocation {
		if ga.State.VoteLastValid > 0 && ga.State.Status == 1 {
			vlv := types.Round(ga.State.VoteLastValid)
			ma := types.MicroAlgos(ga.State.MicroAlgos)
			addr, err := types.DecodeAddress(ga.Address)
			if err == nil {
				onls.log.WithFields(logrus.Fields{"round": 0}).Infof("Genesis stake for %s : %.1f", ga.Address, ma.ToAlgos())
				onls.updateAccount(0, addr, &vlv, &ma)
			}
		}
	}
}

// updateTotals recalculates stake fractions for accounts
// also removes accounts that stopped voting from the state table
func (onls *onlineStakeState) updateTotals(round types.Round) bool {
	var totalStake types.MicroAlgos = 0
	var nextexpiry types.Round = math.MaxInt64

	// only process state table if its dirty or its time to expire a voting key
	if !onls.dirty && onls.NextExpiry > round {
		onls.log.WithFields(logrus.Fields{"round": round}).Debugf("No changes and %d < %d", round, onls.NextExpiry)
		return false
	}

	// calculate
	// - the next closest key expiry round
	// - totalStake
	// mark accounts with expired keys or closed out accounts for deletion
	for _, acc := range onls.Accounts {
		if acc.VoteLast > 0 && acc.VoteLast < nextexpiry {
			nextexpiry = acc.VoteLast + 1
		}
		//move expiry to next update to persist the change
		if acc.Stake == 0 {
			acc.state = Closed
		}
		if acc.VoteLast >= round {
			totalStake += acc.Stake
		} else {
			acc.Stake = 0
			if acc.VoteLast == 0 {
				acc.state = Offlined
			} else {
				acc.state = Expired
			}
		}
		if acc.state != Online {
			onls.log.WithFields(logrus.Fields{"round": round, "addr": acc.Addr}).Infof("Marking for deletion: %s", acc.state.String())
		}
	}
	onls.TotalStake = totalStake
	onls.UpdatedAtRnd = round
	onls.NextExpiry = nextexpiry

	// update stake fractions for all accunts
	for _, acc := range onls.Accounts {
		acc.stakeFraction = float64(acc.Stake) / float64(totalStake)
	}

	// no longer dirty
	onls.dirty = false
	return true
}

// resetAggregate resets aggregate state
func (onls *onlineStakeState) resetAggregate(round types.Round) {
	// remove all accounts marked for deletion in the previous pass
	for addr, acct := range onls.Accounts {
		// remove only if marked as not voting at the end of aggregation bin
		if acct.state != Online {
			onls.log.WithFields(logrus.Fields{"round": round, "addr": acct.Addr}).Infof("Deleting account : %s", acct.state.String())
			delete(onls.Accounts, addr)
		}
		acct.AggOnline = 0
		acct.AggSFSum = 0
	}
}

// updateAggregate update aggregate state
// returns true if the bin is full
func (onls *onlineStakeState) updateAggregate(round types.Round) bool {
	for _, acc := range onls.Accounts {
		if acc.Stake > 0 {
			acc.AggOnline++
			acc.AggSFSum += acc.stakeFraction
		}
	}
	return int64(round)%onls.aggBinSize == onls.aggBinSize-1
}

// updateAccountWithKeyreg updates state with key registration / unregistration (inner)transaction
func (onls *onlineStakeState) updateAccountWithKeyreg(round types.Round, tx *types.SignedTxnWithAD) {
	onls.updateAccount(round, tx.Txn.Sender, &tx.Txn.KeyregTxnFields.VoteLast, nil)
}

// updateAccountWithAcctDelta updates state with account delta (state)
func (onsl *onlineStakeState) updateAccountWithAcctDelta(round types.Round, br *types.BalanceRecord) {
	onsl.updateAccount(round, br.Addr, nil, &br.MicroAlgos)
}

func (onls *onlineStakeState) updateAccount(round types.Round, addr types.Address, voteLast *types.Round, stake *types.MicroAlgos) {
	acct, exists := onls.Accounts[addr]
	updated := false
	if !exists && voteLast == nil {
		return
	}
	if voteLast != nil && *voteLast < round {
		return
	}
	if !exists {
		acct = &partAccount{
			Addr: addr.String(),
		}
		onls.Accounts[addr] = acct
	}
	if stake != nil && acct.Stake != *stake {
		acct.Stake = *stake
		updated = true
		onls.log.WithFields(logrus.Fields{"round": round, "addr": acct.Addr}).Infof("New stake: %.1f", acct.Stake.ToAlgos())
	}
	if voteLast != nil && acct.VoteLast != *voteLast {
		acct.VoteLast = *voteLast - StakeLag
		updated = true
		onls.log.WithFields(logrus.Fields{"round": round, "addr": acct.Addr}).Infof("New voteLast: %d", acct.VoteLast)
	}
	if updated {
		onls.dirty = true
		acct.UpdatedAtRnd = round
	}
}
