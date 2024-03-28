from logging import Logger
from typing import List
import requests

from airflow.hooks.base import BaseHook

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection


class CourierSTGObj():
    def __init__(self, object_id: str, object_value: str) -> None:
         self.object_id = object_id
         self.object_value = object_value


class CourierOriginRepository:
    def list_couriers(self, offset: int, limit: int) -> List[CourierSTGObj]:
        conn = BaseHook.get_connection("DELIVERY_SYSTEM")
        host = conn.host + "/couriers"
        headers = {
            "X-Nickname": "Gera",
            "X-Cohort": "22",
            "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f"
        }
        print(f'Headers: {headers}')
        get_params = {
            "sort_field": "id",
            "sort_direction": "asc",
            "offset": offset,
            "limit": limit
        }
        objs = []
        response = requests.get(host, params=get_params, headers=headers).json()
        for entry in response:
            objs.append(
                CourierSTGObj(
                    entry.get("_id"),
                    entry
                )
            )
        return objs


class CourierDestRepository:

    def insert_courier(self, conn: Connection, courier: CourierSTGObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_couriers(courier_id, object_value)
                    VALUES (%(object_id)s, %(object_value)s);
                """,
                {
                    "object_id": cour   ier.object_id,
                    "object_value": json2str(courier.object_value)
                },
            )


class CourierLoader:
    WF_KEY = "courier_origin_to_stg_workflow"
    OFFSET = "offset"
    BATCH_LIMIT = 50

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.origin = CourierOriginRepository()
        self.pg_dest = pg_dest
        self.stg = CourierDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.OFFSET: 0})

            # Вычитываем очередную пачку объектов.
            offset = wf_setting.workflow_settings[self.OFFSET]
            load_queue = self.origin.list_couriers(offset, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if len(load_queue) == 0:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for courier in load_queue:
                self.stg.insert_courier(conn, courier)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.OFFSET] = offset + len(load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.OFFSET]}")