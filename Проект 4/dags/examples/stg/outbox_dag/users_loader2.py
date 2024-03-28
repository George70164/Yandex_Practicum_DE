from datetime import datetime
import logging
from typing import List
import json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from lib import PgConnect

log = logging.getLogger(__name__)


class OutboxRecord(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str



class EventsLoader:
    def __init__(self, origin_pg: PgConnect, dest_pg: PgConnect):
        self.origin_pg = origin_pg
        self.dest_pg = dest_pg

    def events_load(self):
        log.info("Started events_load")
        with self.dest_pg.connection() as conn:

            try:
                max_loaded_id = self.get_max_loaded_id(conn)
                outbox_records = self.retrieve_outbox_records(max_loaded_id)

                if outbox_records:
                    log.info('into ifka outbox_records')
                    self.insert_events(conn, outbox_records)
                    max_id = max(record.id for record in outbox_records)
                    self.update_max_loaded_id(conn, max_id)

                

            except Exception as e:
                log.error(f"Error occurred during events_load: {str(e)}")
                raise


    def get_max_loaded_id(self, conn: Connection) -> int:
        with self.dest_pg.client().cursor(row_factory=class_row(OutboxRecord)) as cur:
            cur.execute("SELECT workflow_settings->>'max_id' FROM stg.srv_wf_settings WHERE workflow_key = 'bonusystem_events'")
            result = cur.fetchone()
            max_id = int(result[0]) if result is not None else 0
        return max_id

    def retrieve_outbox_records(self, max_loaded_id: int) -> List[OutboxRecord]:
        with self.origin_pg.client().cursor(row_factory=class_row(OutboxRecord)) as cur:
            cur.execute("SELECT * FROM outbox WHERE id > %s", (max_loaded_id,))
            outbox_records = cur.fetchall()
        return outbox_records

    def insert_events(self, conn: Connection, outbox_records: List[OutboxRecord]) -> None:
        with self.dest_pg.client().cursor(row_factory=class_row(OutboxRecord)) as cur:
            for record in outbox_records:
                ###log.info("id in insert_events "+ str(record.id))
                cur.execute("""INSERT INTO stg.bonussystem_events (id, event_ts, event_type, event_value)
                            VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s)""",
                            {
                              "id":record.id,
                              "event_ts":record.event_ts,
                              "event_type":record.event_type,
                              "event_value":record.event_value
                            }
                )
    
                
        

    def update_max_loaded_id(self, conn: Connection, max_id: int) -> None:
        with self.dest_pg.client().cursor(row_factory=class_row(OutboxRecord)) as cur:
            cur.execute("""
            UPDATE stg.srv_wf_settings
            SET workflow_settings = (workflow_settings::text || %s)::jsonb
            WHERE workflow_key = 'bonus_system'
                """, (str(json.dumps({"max_id": max_id})),))
      
    

class UsersLoader:
    def __init__(self, origin_pg: PgConnect, dest_pg: PgConnect):
        self.origin_pg = origin_pg
        self.dest_pg = dest_pg

    def load_users(self):
        # Implement your logic to load users here
        pass