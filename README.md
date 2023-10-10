# Conduit Online Stake Plugin 

**[WARNING] THIS IS W.I.P. - values not verified yet**

This is a Conduit exporter plugin that tracks online state for all participating accounts .

This plugin exports all online stake state to ClickHouse database 
for each round that touches an account with an active keyreg.

For each exported round the total stake and stake fractions are recalculated.

Data is shifted by 320 (except for last voting round) to exactly reflect the the stake as seen by VRF on specific round.

Exported rounds have one extra row with `total` as account address that contains a total online stake for the round.

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

One can query sparse dataset to get the continuous per round stake values 
with the following query:

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

