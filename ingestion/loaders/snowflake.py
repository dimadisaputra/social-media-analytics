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
            schema=settings.snowflake_schema,
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
            logger.warning("No events to load")
            return 0

        df = pd.DataFrame(events)
        df = df.drop_duplicates(subset=["event_id"])
        df["raw_payload"] = df["raw_payload"].apply(json.dumps)
        df["ingested_at"] = datetime.now(timezone.utc)

        df.columns = [col.upper() for col in df.columns]

        try:
            with self._conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE STAGING_RAW_SOCIAL_EVENTS")
                
                write_pandas(
                    conn=self._conn,
                    table_name="STAGING_RAW_SOCIAL_EVENTS",
                    df=df,
                    quote_identifiers=False
                )

                merge_sql = """
                    MERGE INTO RAW_SOCIAL_EVENTS AS target
                    USING STAGING_RAW_SOCIAL_EVENTS AS source
                    ON target.event_id = source.event_id
                    WHEN NOT MATCHED THEN
                        INSERT (event_id, platform, username, entity_type, raw_payload, ingested_at)
                        VALUES (source.event_id, source.platform, source.username, source.entity_type, PARSE_JSON(source.raw_payload), source.ingested_at)
                """
                with self._conn.cursor() as cur:
                    cur.execute(merge_sql)

                logger.info(f"Successfully synced {len(df)} events.")
                return len(df)

        except Exception as e:
            logger.error(f"Failed to sync events into Snowflake: {e}")
            raise e
