from logging import Logger
from typing import List

from examples.to_dds import EtlSetting, EtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class RestaurantObj(BaseModel):
    id: int
    object_value: str


class RestaurantsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, restaurant_threshold: int, limit: int) -> List[RestaurantObj]:
        with self._db.client().cursor(row_factory=class_row(RestaurantObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_value 
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": restaurant_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class RestaurantDestRepository:

    def insert_restaurant(self, conn: Connection, restaurant: RestaurantObj) -> None:
        with conn.cursor() as cur:
            jsonrank = str2json(restaurant.object_value)
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(id, restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(id)s, %(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;
                """,
                {	
               		"id": restaurant.id,
                    "restaurant_id": jsonrank['_id'],  
                    "restaurant_name": jsonrank['name'],
                    "active_from": jsonrank['update_ts'],
                    "active_to": '2099-12-31'
                },
            )


class RestaurantsLoader:
    WF_KEY = "restaurants_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  #  

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = RestaurantsOriginRepository(pg_origin)
        self.dds = RestaurantDestRepository()
        self.settings_repository = EtlSettingsRepository()
        self.log = log

    def load_data(self):
        # открываю транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываю состояние загрузки
            # Если настройки еще нет, создаю ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            self.log.info(f'wf_setting = {wf_setting}')
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываю очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            self.log.info(f'last_loaded = {last_loaded}')
            self.log.info(f'BATCH_LIMIT = {self.BATCH_LIMIT}')
            load_queue = self.origin.list_restaurants(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняю объекты в базу dwh.
            for restaurant in load_queue:
                self.dds.insert_restaurant(conn, restaurant)

            # Сохраняю прогресс.
            # Пользуюсь тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразую к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f'wf_setting_json = {wf_setting_json}')

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
