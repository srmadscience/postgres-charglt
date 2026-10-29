# postgres-charglt
PostgreSQL implementation of [Charglt](https://github.com/srmadscience/voltdb-charglt)

It has the same basic functionality as charglt, adapted for PostgreSQL — see TODO for known gaps.

Far more documentation about the benchmark itself is available at the link above.

## See Also:

* [aerospike-charglt](https://github.com/srmadscience/aerospike-charglt)
* [mongodb-charglt](https://github.com/srmadscience/mongodb-charglt)
* [redis-charglt](https://github.com/srmadscience/redis-charglt)
* [voltdb-charglt](https://github.com/srmadscience/voltdb-charglt)
* [oracle-charglt](https://github.com/srmadscience/oracle-charglt)

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
| `PG_PORT` | `5432` | Port |
| `PG_DATABASE` | `charglt` | Database name |
| `PG_SSLMODE` | driver default | JDBC `sslmode`, e.g. `require` |

## Databricks Lakebase

The benchmark runs unchanged on a Databricks Lakebase Autoscaling project. `lakebase-env.sh` looks up the endpoint's
host, generates a one-hour OAuth token to use as the password, and exports the `PG_*` variables (and `PGHOST` etc.
for `psql`):

```
. ./lakebase-env.sh path/to/databricks.properties   # needs databricks.host and databricks.token
psql -d databricks_postgres -c 'CREATE DATABASE charglt'
psql -d charglt -f ddl/create_db.sql
psql -d charglt -f ddl/create_procs.sql
```

`LAKEBASE_PROJECT` (default `charglt`), `LAKEBASE_BRANCH` (default `production`) and `LAKEBASE_ENDPOINT`
(default `primary`) select the endpoint. The hostname, username and password CLI arguments are then ignored.
Re-source the script before each run: new logins need an unexpired token.

Every call is a network round trip, so when running far from the endpoint use plenty of loader threads and
benchmark workers.

### Example results

These runs used a Free Edition workspace (Postgres 17, 1 CU, AWS us-east-2), with the client on a Mac about
130 ms away (round-trip time). They show the benchmark working, not what Lakebase can do: throughput is limited by
round trips, and grows almost in step with the number of workers.

Loading 10,000 users (`CreateChargingDemoData`):

| Threads | Users per second |
|---|---|
| 1 | ~4 |
| 16 | 58 |

`ChargingDemoTransactions` with 10,000 users, 60 seconds, and 2 tpms requested (so it reports
`UNABLE_TO_MEET_REQUESTED_TPS`):

| Workers | Transactions | TPS | ReportQuotaUsage median / p99 (ms) | AddCredit median / p99 (ms) |
|---|---|---|---|---|
| 16 | 6,018 | 99 | 588 / 1,703 | 571 / 1,691 |
| 100 | 34,698 | 569 | 428 / 1,683 | 417 / 1,658 |

Latencies are measured at the client and include time waiting in the client's queue (a median of 443 ms with
16 workers and 279 ms with 100).

## Running the benchmark

### Load data — CreateChargingDemoData

```
hostname usercount tpms maxinitialcredit username password [threadcount]
```

Example — load 100,000 users at up to 10 transactions per millisecond, each starting with up to 1,000 units of credit:
```
10.13.1.101 100000 10 1000 postgres mypassword
```

`threadcount` (default 1) splits the load across that many threads, each with its own connection.

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
