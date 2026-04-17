# s2-charglt
SingleStore implementation of [Charglt](https://github.com/srmadscience/voltdb-charglt), but on SingleStore

It has the same basic functionality of charglt, but may be missing stuff  - see TODO

Far more documentation is available at the link above.

## See Also:

* [mongodb-charglt](https://github.com/srmadscience/mongodb-charglt)
* [redis-charglt](https://github.com/srmadscience/redis-charglt)
* [voltdb-charglt](https://github.com/srmadscience/voltdb-charglt)

## Installation

1. create a DB called 'charglt'
2. Run [create_db.sql](https://github.com/srmadscience/s2-charglt/blob/main/ddl/create_db.sql) 
3. Run [create_procs.sql](https://github.com/srmadscience/s2-charglt/blob/main/ddl/create_procs.sql)
4. Create 'X' users by running CreateChargingDemoData with:
```
10.13.1.101 100000 10  1000 root nevadaeagle
```
   ... which is hostname, usercount, transactions-per-ms, user, password

5. Then run ChargingDemoTransactions with:

```
10.13.1.101 100000 10 120 30  root nevadaeagle
```

6. ...or the Key Value store demo (ChargingDemoKVStore) with:
```
10.13.1.101 100 1 60 15 100 50 root nevadaeagle
```
## TODO

* I have yet to implement [ShowCurrentAllocations__promBL](https://github.com/srmadscience/voltdb-charglt/blob/master/ddl/create_db.sql#L144), which is used for running totals.
* The original version allows you to send a delta of a JSON object instead of the whole thing. I haven't implemented this.
* This runs either using a single thread doing sync calls, or with multiple worker threads. I have yet to do scaled test of the latter.

## Status 

While this is fine to play with, it's not a fair representation of SingleStore at the moment.


