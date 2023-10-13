# ClickHouse Online Stake Exporter 

An exporter plugin that tracks online state for all participating accounts and exports it to a ClickHouse table.

Plugin exports all state for each round that touches an account with an active keyreg.

# Quickstart

```bash
# create ClickHouse tables (including aggregates) and update cmd/conduit/data/conduit.yml config
make
./cmd/conduit -d cmd/conduit/data
```

# ClickHouse

## Data export
Online stake is exported in **snapshots** only for rounds where there is any change to total online stake.
A snapshot contains all accounts with active keys and non zero stake.

As a special case a 0 microAlgo state is written to DB every time an account stops voting due to :

* 1 round after participation key expires 
* 321 rounds after participation key is unregistered
* 321 rounds after account closes-out

All online stake changes and events  (except key expiration) are shifted 320 rounds to match the algod VRF input. 

See [To VRF on not to Vote article](https://medium.com/@ppierscionek/to-vrf-or-not-aabccbe3bd25) for more information. 
# Extra snapshot row

Exported rounds have one extra row with "total" as account address.
This entry contains a total online stake for the round.

Timestamps are not exported as they are unavailable - due to 320 round shift.

## DDL

Tune the following DDL to your specific needs before running the plugin. 
```sql
CREATE TABLE online_stake
(
	addr LowCardinality(String) CODEC(ZSTD(1)),
	round UInt64 CODEC(Delta, ZSTD(1)),
	microAlgos Int64,
	stakeFraction Float64,
	index rnd round TYPE minmax GRANULARITY 4	
) engine = MergeTree()
    PARTITION BY (intDiv(round, 1000000))
    ORDER BY (addr, round);
```

Choose partitioning , expiration, clustering/ordering and indexing that best suits your use case.  

## Queries

Get continuous, per round, stake for an account with the following query:

```sql
SELECT 
  * 
FROM
  online_stake 
WHERE
  addr = 'DTHIRTEENUHXDHS7IZZBUPHXYWNT5TSSAAUX6NKTLJBR5ABOPTHNEA4DCU'
ORDER BY
  round WITH FILL INTERPOLATE 
```

## Aggregates DDL

```sql
CREATE TABLE online_stake_ag10
(
	addr LowCardinality(String) CODEC(ZSTD(1)),
	round UInt64 CODEC(Delta, ZSTD(1)),
	rndsOnline Int32 CODEC(ZSTD(1)),
	sfSum Float64 CODEC(ZSTD(1)),
	index rnd round TYPE minmax GRANULARITY 4,
) engine = MergeTree()
    ORDER BY (addr, round);

CREATE TABLE online_stake_ag1k
(
	addr LowCardinality(String) CODEC(ZSTD(1)),
	round UInt64 CODEC(Delta, ZSTD(1)),
	rndsOnline SimpleAggregateFunction(sum, Int64) CODEC(ZSTD(1)),
	sfSum SimpleAggregateFunction(sum, Float64) CODEC(ZSTD(1)),
	index rnd round TYPE minmax GRANULARITY 4,
) engine = SummingMergeTree()
    ORDER BY (addr, round);

CREATE MATERIALIZED VIEW mv_online_stake_ag1k to online_stake_ag1k
AS SELECT
	addr
	, intDiv(round,1000) round
	, sum(rndsOnline) rndsOnline
	, sum(sfSum) sfSum
	FROM 
		online_stake_ag10
	GROUP BY 
		addr,round;
   
CREATE TABLE voi_testnet.online_stake_ag100k
(
	addr LowCardinality(String) CODEC(ZSTD(1)),
	round UInt64 CODEC(Delta, ZSTD(1)),
	rndsOnline SimpleAggregateFunction(sum, Int64) CODEC(ZSTD(1)),
	sfSum SimpleAggregateFunction(sum, Float64) CODEC(ZSTD(1)),
	index rnd round TYPE minmax GRANULARITY 4,
) engine = SummingMergeTree()
    ORDER BY (addr, round);

CREATE MATERIALIZED VIEW mv_online_stake_ag100k to online_stake_ag100k
AS SELECT
	addr
	, intDiv(round,100000) round
	, sum(rndsOnline) rndsOnline
	, sum(sfSum) sfSum
	FROM 
		online_stake_ag10
	GROUP BY 
		addr,round;
```
