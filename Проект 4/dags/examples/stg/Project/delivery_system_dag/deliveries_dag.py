

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from examples.stg.delivery_system_dag.couriers_saver import CourierClass
from examples.stg.delivery_system_dag.delivery_saver import DeliveryClass

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

from lib import ConnectionBuilder


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'project', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_project_stg():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def courier_task():
        courier_obj = CourierClass(dwh_pg_connect)
        courier_obj.get_data()

    @task()
    def delivery_task():   
        delivery_obj = DeliveryClass(dwh_pg_connect)
        delivery_obj.get_data()

    task_courier = courier_task()
    task_delivery = delivery_task()


    task_courier >> task_delivery


deliveries_stg_dag = sprint5_project_stg()  