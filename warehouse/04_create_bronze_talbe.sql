CREATE TABLE IF NOT EXISTS SOCIAL_MEDIA_DW.BRONZE.RAW_SOCIAL_EVENTS (
  event_id STRING,
  platform STRING,
  username STRING,
  entity_type STRING, -- post / comment
  raw_payload VARIANT,
  ingested_at TIMESTAMP_NTZ
);
