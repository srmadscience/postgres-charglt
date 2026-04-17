-- PostgreSQL PL/pgSQL stored functions.
-- Run this script connected to the 'charglt' database after running create_db.sql.
-- All status-returning functions return TABLE(l_status_byte INTEGER, l_status_string TEXT).
-- Call them from Java with: SELECT * FROM FunctionName(?, ...)


CREATE OR REPLACE FUNCTION SendToKafka(p_userid bigint, p_txnId TEXT)
RETURNS VOID AS $$
BEGIN
  -- Do nothing unless replaced by send_to_kafka.sql
  NULL;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION GetUsersWithMultipleSessions()
RETURNS TABLE(userid BIGINT, how_many BIGINT) AS $$
BEGIN
  RETURN QUERY
    SELECT us.userid, us.how_many
    FROM users_sessions us
    WHERE us.how_many > 1
    ORDER BY us.how_many, us.userid
    LIMIT 50;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION showTransactions(p_userid bigint)
RETURNS TABLE(userid BIGINT, user_txn_id VARCHAR, txn_time TIMESTAMP,
              sessionid BIGINT, approved_amount BIGINT, spent_amount BIGINT, purpose VARCHAR) AS $$
BEGIN
  RETURN QUERY
    SELECT urt.userid, urt.user_txn_id, urt.txn_time, urt.sessionid,
           urt.approved_amount, urt.spent_amount, urt.purpose
    FROM user_recent_transactions urt
    WHERE urt.userid = p_userid
    ORDER BY urt.txn_time, urt.user_txn_id;
END;
$$ LANGUAGE plpgsql;


-- Returns one row per table (user_table, user_usage_table, user_recent_transactions)
-- so callers iterating rows get 3 rows for a user with one record in each table.
CREATE OR REPLACE FUNCTION GetUser(p_userid bigint)
RETURNS TABLE(userid BIGINT) AS $$
BEGIN
  RETURN QUERY SELECT ut.userid FROM user_table ut WHERE ut.userid = p_userid;
  RETURN QUERY SELECT uut.userid FROM user_usage_table uut WHERE uut.userid = p_userid ORDER BY uut.sessionid;
  RETURN QUERY SELECT urt.userid FROM user_recent_transactions urt WHERE urt.userid = p_userid ORDER BY urt.txn_time, urt.user_txn_id;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION GetAndLockUser(p_userid bigint, p_new_lock_id bigint)
RETURNS TABLE(l_status_byte INTEGER, l_status_string TEXT) AS $$
DECLARE
  l_sb INTEGER := 42;
  l_ss TEXT := 'OK';
  l_found_userid BIGINT := NULL;
  l_user_softlock_expiry TIMESTAMP := NULL;
  l_user_softlock_sessionid BIGINT := NULL;
BEGIN
  SELECT ut.userid, ut.user_softlock_expiry, ut.user_softlock_sessionid
  INTO l_found_userid, l_user_softlock_expiry, l_user_softlock_sessionid
  FROM user_table ut
  WHERE ut.userid = p_userid;

  IF l_found_userid = p_userid THEN
    IF l_user_softlock_sessionid = p_new_lock_id
       OR l_user_softlock_expiry IS NULL
       OR l_user_softlock_expiry < CLOCK_TIMESTAMP() THEN
      UPDATE user_table
      SET user_softlock_sessionid = p_new_lock_id,
          user_softlock_expiry = CLOCK_TIMESTAMP() + INTERVAL '1 second'
      WHERE userid = p_userid;
      l_sb := 54;
      l_ss := 'User ' || p_userid || ' locked by session ' || l_user_softlock_sessionid;
    ELSE
      l_sb := 53;
      l_ss := 'User ' || p_userid || ' already locked by session ' || l_user_softlock_sessionid;
    END IF;
  ELSE
    l_sb := 50;
    l_ss := 'User ' || p_userid || ' does not exist';
  END IF;

  RETURN QUERY SELECT l_sb, l_ss;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION UpdateLockedUser(p_userid bigint, p_new_lock_id bigint,
                                             p_json_payload TEXT, p_delta_operation_name TEXT)
RETURNS TABLE(l_status_byte INTEGER, l_status_string TEXT) AS $$
DECLARE
  l_sb INTEGER := 0;
  l_ss TEXT := '';
  l_found_userid BIGINT := NULL;
  l_user_softlock_expiry TIMESTAMP := NULL;
  l_user_softlock_sessionid BIGINT := NULL;
BEGIN
  SELECT ut.userid, ut.user_softlock_expiry, ut.user_softlock_sessionid
  INTO l_found_userid, l_user_softlock_expiry, l_user_softlock_sessionid
  FROM user_table ut
  WHERE ut.userid = p_userid;

  IF l_found_userid = p_userid THEN
    IF l_user_softlock_sessionid = p_new_lock_id
       OR l_user_softlock_expiry IS NULL
       OR l_user_softlock_expiry < CLOCK_TIMESTAMP() THEN
      IF p_delta_operation_name = 'NEW_LOYALTY_NUMBER' THEN
        UPDATE user_table
        SET user_softlock_sessionid = NULL,
            user_softlock_expiry = NULL,
            user_json_object = jsonb_set(
              COALESCE(user_json_object, '{}'::jsonb),
              '{loyaltySchemeNumber}',
              to_jsonb(p_json_payload::BIGINT))
        WHERE userid = p_userid;
      ELSE
        UPDATE user_table
        SET user_softlock_sessionid = NULL,
            user_softlock_expiry = NULL,
            user_json_object = p_json_payload::JSONB
        WHERE userid = p_userid;
      END IF;
      l_sb := 42;
      l_ss := 'User ' || p_userid || ' updated';
    ELSE
      l_sb := 53;
      l_ss := 'User ' || p_userid || ' already locked by session ' || l_user_softlock_sessionid;
    END IF;
  ELSE
    l_sb := 50;
    l_ss := 'User ' || p_userid || ' does not exist';
  END IF;

  RETURN QUERY SELECT l_sb, l_ss;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION UpsertUser(p_userid BIGINT, p_addBalance BIGINT, p_json TEXT,
                                       p_purpose TEXT, p_lastSeen TIMESTAMP, p_txnId TEXT)
RETURNS TABLE(l_status_byte INTEGER, l_status_string TEXT) AS $$
DECLARE
  l_sb INTEGER := 0;
  l_ss TEXT := '';
  l_found_userid BIGINT := NULL;
  l_found_txn_id TEXT := NULL;
BEGIN
  SELECT ut.userid INTO l_found_userid FROM user_table ut WHERE ut.userid = p_userid;
  SELECT urt.user_txn_id INTO l_found_txn_id
  FROM user_recent_transactions urt
  WHERE urt.userid = p_userid AND urt.user_txn_id = p_txnId;

  IF l_found_txn_id IS NOT NULL AND l_found_txn_id = p_txnId THEN
    l_sb := 46;
    l_ss := 'Txn ' || p_txnId || ' already happened';
  ELSE
    IF l_found_userid = p_userid THEN
      UPDATE user_table
      SET user_json_object = p_json::JSONB,
          user_last_seen = p_lastSeen,
          user_balance = p_addBalance,
          user_softlock_expiry = NULL,
          user_softlock_sessionid = NULL
      WHERE userid = p_userid;
      l_ss := 'User ' || p_userid || ' updated';
    ELSE
      INSERT INTO user_table (userid, user_json_object, user_last_seen, user_balance)
      VALUES (p_userid, p_json::JSONB, p_lastSeen, p_addBalance);
      l_ss := 'User ' || p_userid || ' inserted';
    END IF;
    l_sb := 42;
    INSERT INTO user_recent_transactions (userid, user_txn_id, txn_time, approved_amount, spent_amount, purpose)
    VALUES (p_userid, p_txnId, p_lastSeen, 0, p_addBalance, 'Create User');
    PERFORM SendToKafka(p_userid, p_txnId);
  END IF;

  RETURN QUERY SELECT l_sb, l_ss;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION DelUser(p_userid bigint)
RETURNS VOID AS $$
BEGIN
  DELETE FROM user_table WHERE userid = p_userid;
  DELETE FROM user_usage_table WHERE userid = p_userid;
  DELETE FROM user_recent_transactions WHERE userid = p_userid;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ReportQuotaUsage(p_userid bigint, p_units_used bigint,
                                             p_units_wanted bigint, p_sessionid bigint, p_txnId TEXT)
RETURNS TABLE(l_status_byte INTEGER, l_status_string TEXT) AS $$
DECLARE
  l_sb INTEGER := 0;
  l_ss TEXT := '';
  l_found_userid BIGINT := NULL;
  l_found_txn_id TEXT := NULL;
  l_balance BIGINT := 0;
  l_allocated_amount BIGINT := 0;
  l_amount_spent BIGINT;
  l_available_credit BIGINT := 0;
  l_offered_credit BIGINT := 0;
BEGIN
  l_amount_spent := p_units_used * -1;

  SELECT ut.userid, ut.user_balance
  INTO l_found_userid, l_balance
  FROM user_table ut
  WHERE ut.userid = p_userid;

  SELECT urt.user_txn_id INTO l_found_txn_id
  FROM user_recent_transactions urt
  WHERE urt.userid = p_userid AND urt.user_txn_id = p_txnId;

  SELECT COALESCE(SUM(uut.allocated_amount), 0) INTO l_allocated_amount
  FROM user_usage_table uut
  WHERE uut.userid = p_userid AND uut.sessionid != p_sessionid;

  IF l_found_txn_id = p_txnId THEN
    l_sb := 46;
    l_ss := 'Txn ' || p_txnId || ' already happened';
  ELSE
    IF l_found_userid = p_userid THEN
      DELETE FROM user_usage_table WHERE userid = p_userid AND sessionid = p_sessionid;

      l_available_credit := l_balance + l_amount_spent - l_allocated_amount;

      IF l_available_credit < 0 THEN
        l_sb := 43;
        l_ss := 'Negative balance: ' || l_available_credit;
      ELSE
        IF p_units_wanted > l_available_credit THEN
          l_offered_credit := l_available_credit;
          l_sb := 44;
          l_ss := l_offered_credit || ' of ' || p_units_wanted || ' Allocated';
        ELSE
          l_offered_credit := p_units_wanted;
          l_sb := 45;
          l_ss := l_offered_credit || ' Allocated';
        END IF;

        INSERT INTO user_recent_transactions
          (userid, user_txn_id, txn_time, approved_amount, spent_amount, purpose)
        VALUES
          (p_userid, p_txnId, CLOCK_TIMESTAMP(), l_offered_credit, l_amount_spent, 'Spend');

        PERFORM SendToKafka(p_userid, p_txnId);
      END IF;

      UPDATE user_table
      SET user_balance = user_balance + l_amount_spent
      WHERE userid = p_userid;

      IF l_offered_credit > 0 THEN
        INSERT INTO user_usage_table (userid, allocated_amount, sessionid, lastdate)
        VALUES (p_userid, l_offered_credit, p_sessionid, CLOCK_TIMESTAMP());
      END IF;

      DELETE FROM user_recent_transactions
      WHERE userid = p_userid
        AND txn_time < CLOCK_TIMESTAMP() - INTERVAL '1 second';
    ELSE
      l_sb := 50;
      l_ss := 'User ' || p_userid || ' does not exist';
    END IF;
  END IF;

  RETURN QUERY SELECT l_sb, l_ss;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION AddCredit(p_userid bigint, p_extra_credit bigint, p_txnId TEXT)
RETURNS TABLE(l_status_byte INTEGER, l_status_string TEXT) AS $$
DECLARE
  l_sb INTEGER := 0;
  l_ss TEXT := '';
  l_found_userid BIGINT := NULL;
  l_found_txn_id TEXT := NULL;
BEGIN
  SELECT ut.userid INTO l_found_userid FROM user_table ut WHERE ut.userid = p_userid;
  SELECT urt.user_txn_id INTO l_found_txn_id
  FROM user_recent_transactions urt
  WHERE urt.userid = p_userid AND urt.user_txn_id = p_txnId;

  IF l_found_txn_id = p_txnId THEN
    l_sb := 46;
    l_ss := 'Txn ' || p_txnId || ' already happened';
  ELSE
    IF l_found_userid = p_userid THEN
      UPDATE user_table
      SET user_balance = user_balance + p_extra_credit
      WHERE userid = p_userid;

      INSERT INTO user_recent_transactions
        (userid, user_txn_id, txn_time, approved_amount, spent_amount, purpose)
      VALUES
        (p_userid, p_txnId, CLOCK_TIMESTAMP(), 0, p_extra_credit, 'Add Credit');

      PERFORM SendToKafka(p_userid, p_txnId);

      DELETE FROM user_recent_transactions
      WHERE userid = p_userid
        AND txn_time < CLOCK_TIMESTAMP() - INTERVAL '1 second';

      l_sb := 56;
      l_ss := p_extra_credit || ' added by Txn ' || p_txnId;
    ELSE
      l_sb := 50;
      l_ss := 'User ' || p_userid || ' does not exist';
    END IF;
  END IF;

  RETURN QUERY SELECT l_sb, l_ss;
END;
$$ LANGUAGE plpgsql;
