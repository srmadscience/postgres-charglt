# postgres-charglt
PostgreSQL implementation of [Charglt](https://github.com/srmadscience/voltdb-charglt)

It has the same basic functionality as charglt, adapted for PostgreSQL — see TODO for known gaps.

Far more documentation about the benchmark itself is available at the link above.

## See Also:

* [mongodb-charglt](https://github.com/srmadscience/mongodb-charglt)
* [redis-charglt](https://github.com/srmadscience/redis-charglt)
* [voltdb-charglt](https://github.com/srmadscience/voltdb-charglt)

## Prerequisites

* PostgreSQL 12 or later (generated column support required)
* Java 21
* Maven

## Installation

1. Create the database:
```sql
CREATE DATABASE charglt;
```

2. Connect to the `charglt` database and run the schema DDL:
```
psql -d charglt -f ddl/create_db.sql
```

3. Run the stored functions DDL:
```
psql -d charglt -f ddl/create_procs.sql
```

4. Optionally enable Kafka event streaming (uses `pg_notify` by default):
```
psql -d charglt -f ddl/send_to_kafka.sql
```

## Configuration

Connection details are set via environment variables:

| Variable | Default (fallback) | Description |
|---|---|---|
| `PG_HOST` | first CLI argument | PostgreSQL host (single host or comma-separated list for failover) |
| `PG_USER` | CLI argument | Database user |
| `PG_PASSWORD` | CLI argument | Database password |

Port defaults to **5432**. The database name is always `charglt`.

## Running the benchmark

### Load data — CreateChargingDemoData

```
hostname usercount tpms maxinitialcredit username password
```

Example — load 100,000 users at up to 10 transactions per millisecond, each starting with up to 1,000 units of credit:
```
10.13.1.101 100000 10 1000 postgres mypassword
```

### Transaction benchmark — ChargingDemoTransactions

```
hostname usercount tpms durationseconds queryseconds username password workercount
```

Example — run against 100,000 users at 10 tpms for 120 seconds, printing global stats every 30 seconds, using a single thread:
```
10.13.1.101 100000 10 120 30 postgres mypassword 0
```

### Key-Value store benchmark — ChargingDemoKVStore

```
hostname usercount tpms durationseconds queryseconds jsonsize deltaproportion username password workercount
```

Example — 100 users, 1 tpms, 60 seconds, stats every 15 seconds, 100-byte JSON payloads, 50% delta updates:
```
10.13.1.101 100 1 60 15 100 50 postgres mypassword 0
```

### Delete data — DeleteChargingDemoData

```
hostname usercount tpms username password
```

## Schema notes

* `user_table.user_json_object` is stored as `JSONB`.
* `user_json_cardid` is a generated column (`BIGINT`) computed from `user_json_object->>'loyaltySchemeNumber'`, automatically indexed.
* All stored procedures are implemented as PL/pgSQL **functions** and are called with `SELECT * FROM FunctionName(...)`.
* Kafka integration is stubbed using `pg_notify` on the `charglt` channel. Use `LISTEN charglt` in a client to receive events, or replace `send_to_kafka.sql` with a real Kafka FDW integration.

## Removing the schema

```
psql -d charglt -f ddl/remove_db.sql
```

## TODO

* Implement `ShowCurrentAllocations` (running totals), present in the VoltDB original.
* Scaled multi-worker tests have not yet been validated end-to-end.
