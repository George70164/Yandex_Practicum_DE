from datetime import datetime
from typing import List, Optional, Any

from lib.dict_util import json2str
from psycopg import Connection
from lib import PgConnect
from pydantic import BaseModel
import json

import logging
import requests
from datetime import datetime, timedelta

log = logging.getLogger(__name__)
url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries'
headers = {
    'X-Nickname': 'Gera',
    'X-Cohort': '22',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
}
params = {
    'sort_field': 'id',
    'sort_direction': 'asc',
    'limit': 50,
    'offset': 0,
    'from': (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
}


class DeliveryObj(BaseModel):
    # object_id: str
    object_value: str


class DeliveryClass:

    def __init__(self, pg: PgConnect):
        self.dwh = pg

    # def clear_data(self):
    #     with self.dwh.connection() as conn:
    #         with conn.cursor() as cur:
    #             cur.execute(
    #                 """
    #                     DELETE FROM stg.deliverysystem_couriers
    #                 """,
    #             )

    def get_data(self):

        params['offset'] = 0
        params['limit'] = 50
        while True:
            response = requests.get(url, headers=headers, params=params)
            data = response.json()
            result = str(data).replace("'", '"')
            log.info(result)
            # break
            if len(result) < 3:
                break
            else:
                objects = json.loads(result)
                deliveries = []
                for obj in objects:
                    delivery = DeliveryObj(object_value=str(obj))
                    log.info(str(obj))
                    deliveries.append(delivery)
                    
                self.insert_delivery(deliveries)
                params['offset'] = params['offset'] + params['limit']

    def insert_delivery(self, deliveries: List[DeliveryObj]) -> None:
        with self.dwh.connection() as conn:
            with conn.cursor() as cur:
                for delivery in deliveries:
                    cur.execute(
                        """
                            INSERT INTO stg.deliverysystem_deliveries(
                                object_value, 
                                update_ts)
                            VALUES (
                                %(object_value)s,
                                NOW())
                            ON CONFLICT (object_value) DO UPDATE
                            SET
                                object_value = EXCLUDED.object_value,
                                update_ts = NOW()
                    ;
                            ;
                        """,
                        {
                            "object_value": delivery.object_value
                        },
                    )