import json
import os
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector
from loguru import logger
from snowflake.connector.pandas_tools import write_pandas
from config import settings


class SnowflakeLoader:
    def __init__(self):
        self._conn = snowflake.connector.connect(
            user=settings.snowflake_user,
            account=settings.snowflake_account,
            private_key=self.load_private_key(settings.snowflake_private_key_path),
            warehouse=settings.snowflake_warehouse,
            database=settings.snowflake_database,
            schema=settings.snowflake_bronze_schema,
            role=settings.snowflake_role,
        )

    @staticmethod
    def load_private_key(key_path: str):
        path = os.path.expanduser(key_path)
        with open(path, "rb") as key_file:
            return serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )

    def close(self):
        self._conn.close()

    def load_events(self, events: List[Dict]) -> int:
        if not events:
            logger.warning("No events to load.")
            return 0

        # --- Build DataFrame ---
        df = pd.DataFrame(events)
        df = df.drop_duplicates(subset=["event_id"])
        df["raw_payload"] = df["raw_payload"].apply(json.dumps)  # VARIANT-safe: keep as STRING in staging
        df["ingested_at"] = datetime.now(timezone.utc)
        df.columns = [col.upper() for col in df.columns]

        try:
            cur = self._conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS STAGING_RAW_SOCIAL_EVENTS (
                    event_id    STRING,
                    platform    STRING,
                    username    STRING,
                    entity_type STRING,
                    raw_payload STRING,
                    ingested_at TIMESTAMP_NTZ
                )
            """)

            cur.execute("TRUNCATE TABLE STAGING_RAW_SOCIAL_EVENTS")

            success, num_chunks, num_rows, output = write_pandas(
                conn=self._conn,
                table_name="STAGING_RAW_SOCIAL_EVENTS",
                df=df,
                quote_identifiers=False,
            )

            if not success:
                raise RuntimeError(f"write_pandas failed — chunks: {num_chunks}, rows: {num_rows}, output: {output}")

            logger.info(f"Staged {num_rows} rows into STAGING_RAW_SOCIAL_EVENTS.")

            cur.execute("""
                MERGE INTO RAW_SOCIAL_EVENTS AS target
                USING STAGING_RAW_SOCIAL_EVENTS AS source
                    ON target.event_id = source.event_id
                WHEN NOT MATCHED THEN
                    INSERT (event_id, platform, username, entity_type, raw_payload, ingested_at)
                    VALUES (
                        source.event_id,
                        source.platform,
                        source.username,
                        source.entity_type,
                        PARSE_JSON(source.raw_payload),
                        source.ingested_at
                    )
            """)

            merged_rows = cur.rowcount
            logger.info(f"MERGE complete — {merged_rows} new events inserted into RAW_SOCIAL_EVENTS.")
            return merged_rows

        except Exception as e:
            logger.error(f"Failed to sync events into Snowflake: {e}")
            raise

        finally:
            cur.close()