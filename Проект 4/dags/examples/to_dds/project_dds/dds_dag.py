import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.utils.task_group import TaskGroup

from lib import ConnectionBuilder
from examples.to_dds.project_dds.couriers_loader import DDSCourierLoader
from examples.to_dds.project_dds.deliveries_loader import DDSDeliveryLoader


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждые 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_project_stg_to_dds2():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    
    @task(task_id="couriers_load")
    def load_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        courier_loader = DDSCourierLoader(dwh_pg_connect, log)
        courier_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
    couriers_dict = load_couriers()

    @task(task_id="deliveries_load")
    def load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        delivery_loader = DDSDeliveryLoader(dwh_pg_connect, log)
        delivery_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

        # Инициализируем объявленные таски.
    deliveries_dict = load_deliveries()

    couriers_dict >> deliveries_dict 


sprint5_project_stg_to_dds = sprint5_project_stg_to_dds2()