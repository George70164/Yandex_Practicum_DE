from datetime import datetime
from typing import List, Optional, Any

from lib.dict_util import json2str
from lib import PgConnect
from pydantic import BaseModel
import json

import logging
import requests


log = logging.getLogger(__name__)
url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers'
headers = {
    'X-Nickname': 'Gera',
    'X-Cohort': '22',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
}
params = {
    'sort_field': 'id',
    'sort_direction': 'asc',
    'limit': 50,
    'offset': 0
}


class CourierObj(BaseModel):
    # object_id: str
    object_value: str


class CourierClass:

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
            if len(result) < 3:
                break
            else:
                objects = json.loads(result)
                couriers = []
                for obj in objects:
                    courier = CourierObj(object_value=str(obj))
                    log.info(str(obj))
                    couriers.append(courier)
                self.insert_courier(couriers)
                params['offset'] = params['offset'] + params['limit']

    def insert_courier(self, couriers: List[CourierObj]) -> None:
        with self.dwh.connection() as conn:
            with conn.cursor() as cur:
                for courier in couriers:
                    cur.execute(
                        """
                            INSERT INTO stg.deliverysystem_couriers(object_value, update_ts)
                            VALUES (%(object_value)s,NOW())
                            ON CONFLICT (object_value) DO UPDATE
                            SET
                                object_value = EXCLUDED.object_value,
                                update_ts = NOW();
                        """,
                        {
                            "object_value": courier.object_value
                        },
                    )