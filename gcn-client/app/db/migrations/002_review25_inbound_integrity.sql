ALTER TABLE gcn_inbound_notices
  MODIFY COLUMN raw_payload LONGBLOB NOT NULL;

ALTER TABLE gcn_inbound_notices
  ADD COLUMN idempotency_key VARCHAR(512) NULL AFTER kafka_timestamp;

ALTER TABLE gcn_inbound_notices
  ADD UNIQUE KEY uq_inbound_idempotency_key (idempotency_key);
