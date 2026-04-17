
-- PostgreSQL does not have native Kafka integration built in.
-- Install a Kafka foreign data wrapper (e.g. kafka_fdw) or use
-- pg_notify / LISTEN/NOTIFY for lightweight event streaming.
--
-- This stub replaces the no-op SendToKafka with a version that
-- uses NOTIFY so consumers can pick up events via LISTEN.
-- Adjust the channel name and payload to suit your consumer.

CREATE OR REPLACE FUNCTION SendToKafka(p_userid bigint, p_txnId TEXT)
RETURNS VOID AS $$
BEGIN
  PERFORM pg_notify(
    'charglt',
    json_build_object('userid', p_userid, 'txnId', p_txnId)::TEXT
  );
END;
$$ LANGUAGE plpgsql;
