
-- Run this script connected to the 'charglt' database.
-- Create the database first with: CREATE DATABASE charglt;
CREATE TABLE user_table
(userid bigint not null primary key
,user_last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
,user_softlock_sessionid bigint
,user_softlock_expiry TIMESTAMP
,user_balance bigint not null
,user_json_object JSONB
,user_json_cardid BIGINT GENERATED ALWAYS AS ((user_json_object->>'loyaltySchemeNumber')::BIGINT) STORED
);

CREATE INDEX ut_del ON user_table(user_last_seen);
CREATE INDEX ut_loyaltycard ON user_table(user_json_cardid);

CREATE TABLE user_usage_table
(userid bigint not null
,allocated_amount bigint not null
,sessionid bigint not null
,lastdate timestamp not null
,PRIMARY KEY (userid, sessionid)
);

CREATE INDEX ust_del_idx1 ON user_usage_table(lastdate);
CREATE INDEX uut_ix1 ON user_usage_table(userid, lastdate);

CREATE TABLE user_recent_transactions
(userid bigint not null
,user_txn_id varchar(128) NOT NULL
,txn_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP not null
,sessionid bigint
,approved_amount bigint
,spent_amount bigint
,purpose varchar(128)
,PRIMARY KEY (userid, user_txn_id)
);

CREATE INDEX urt_del_idx ON user_recent_transactions(userid, txn_time, user_txn_id);
CREATE INDEX urt_del_idx3 ON user_recent_transactions(txn_time);


CREATE VIEW current_locks AS
SELECT COUNT(*) AS how_many
FROM user_table
WHERE user_softlock_expiry IS NOT NULL;

CREATE VIEW allocated_credit AS
SELECT SUM(allocated_amount) AS allocated_amount
FROM user_usage_table;

CREATE VIEW users_sessions AS
SELECT userid, COUNT(*) AS how_many
FROM user_usage_table
GROUP BY userid;


CREATE VIEW recent_activity_out AS
SELECT DATE_TRUNC('minute', txn_time) AS txn_time
       , SUM(approved_amount * -1) AS approved_amount
       , SUM(spent_amount) AS spent_amount
       , COUNT(*) AS how_many
FROM user_recent_transactions
WHERE spent_amount <= 0
GROUP BY DATE_TRUNC('minute', txn_time);

CREATE VIEW recent_activity_in AS
SELECT DATE_TRUNC('minute', txn_time) AS txn_time
       , SUM(approved_amount) AS approved_amount
       , SUM(spent_amount) AS spent_amount
       , COUNT(*) AS how_many
FROM user_recent_transactions
WHERE spent_amount > 0
GROUP BY DATE_TRUNC('minute', txn_time);


CREATE VIEW cluster_activity_by_users AS
SELECT userid, COUNT(*) AS how_many
FROM user_recent_transactions
GROUP BY userid;

CREATE VIEW cluster_activity AS
SELECT DATE_TRUNC('minute', txn_time) AS txn_time, COUNT(*) AS how_many
FROM user_recent_transactions
GROUP BY DATE_TRUNC('minute', txn_time);

CREATE VIEW last_cluster_activity AS
SELECT MAX(txn_time) AS txn_time
FROM user_recent_transactions;

CREATE VIEW cluster_users AS
SELECT COUNT(*) AS how_many
FROM user_table;
