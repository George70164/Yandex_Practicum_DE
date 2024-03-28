from logging import Logger
from typing import List

from examples.to_dds import EtlSetting, EtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class SalesObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class SalesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_objects(self, sales_threshold: int, limit: int) -> List[SalesObj]:
        with self._db.client().cursor(row_factory=class_row(SalesObj)) as cur:
            cur.execute(
                """
                    WITH 
                        t1 as (
                            select
                                os.object_id,
                                do2.id as order_id,
                                json_array_elements(os.object_value::json -> 'order_items') ->> 'id' as product_id,
                                cast(json_array_elements(os.object_value::json -> 'order_items') ->> 'quantity' AS int) as count,
                                cast(json_array_elements(os.object_value::json -> 'order_items') ->> 'price' AS numeric(19, 5)) as price
                            from stg.ordersystem_orders os
                            join dds.dm_orders do2 on os.object_id = do2.order_key
                        ),
                        t2 as (
                            select 
                                be.event_value::json ->> 'order_id' as object_id,
                                json_array_elements(be.event_value::json -> 'product_payments') ->> 'product_id' as product_id,
                                cast(json_array_elements(be.event_value::json -> 'product_payments') ->> 'bonus_payment' AS numeric(19, 5)) as bonus_payment,
                                cast(json_array_elements(be.event_value::json -> 'product_payments') ->> 'bonus_grant' AS numeric(19, 5)) as bonus_grant
                            from stg.bonussystem_events be
                        )
                        select  
                            ROW_NUMBER() OVER () AS id,
                            dp.id as product_id,
                            table1.order_id,
                            table1.count,
                            table1.price,
                            table1.price * table1.count as total_sum,
                            table2.bonus_payment,
                            table2.bonus_grant
                        from t1 table1
                        join t2 table2 on table1.object_id = table2.object_id and table1.product_id = table2.product_id
                        join dds.dm_products dp on table1.product_id = dp.product_id
                        WHERE id > %(threshold)s --Пропускаю те объекты, которые уже загрузили.
                        ORDER BY id ASC --Обязательна сортировка по id, т.к. id используется в качестве курсора.
                        LIMIT %(limit)s --Обрабатываю только одну пачку объектов.;
                """, {
                    "threshold": sales_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class SalesDestRepository:

    def insert_object(self, conn: Connection, sale: SalesObj) -> None:

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(id, product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (DEFAULT, %(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        product_id = EXCLUDED.product_id,
                        order_id = EXCLUDED.order_id,
                        count = EXCLUDED.count,
                        price = EXCLUDED.price,
                        total_sum = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant;
                    """,
                    {
                        "product_id": sale.product_id,
                        "order_id": sale.order_id,
                        "count": sale.count,
                        "price": sale.price,
                        "total_sum": sale.total_sum,
                        "bonus_payment": sale.bonus_payment,
                        "bonus_grant": sale.bonus_grant
                    },
                )




class ProductSalesLoader:
    WF_KEY = "sales_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100000  #  

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = SalesOriginRepository(pg_origin)
        self.dds = SalesDestRepository()
        self.settings_repository = EtlSettingsRepository()
        self.log = log

    def load_data(self):
        # Открываю транзакцию.
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
            load_queue = self.stg.list_objects(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняю объекты в базу dwh.
            for object in load_queue:
                try:
                    self.dds.insert_object(conn, object)
                except Exception as err:
                    print(object)
                    print('Error =', err)   
                    raise  

            # Сохраняю прогресс.
            # Пользуюсь тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразую к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
            self.log.info(f'wf_setting_json = {wf_setting_json}')

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
