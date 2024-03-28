import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.Project.couriers_deliveries.api_loader import APIClient, APIDataLoader
from datetime import datetime
from dateutil.parser import parse


from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2024, 3, 14, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_project_api_loader():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    base_url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/'
    headers = {'X-Nickname': 'Gera', 'X-Cohort': '22', 'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'}
    
    @task()
    def load_couriers():
        params_couriers = {'sort_field': 'id','sort_direction': 'asc', 'limit' : 50, 'offset' : 0}
        apiclient = APIClient(base_url, dwh_pg_connect, 'deliverysystem_couriers', headers)
        load_queue = apiclient.get('couriers', params_couriers  )
        print('Данные для загрузки: ', load_queue)
        dataLoader = APIDataLoader(apiclient, dwh_pg_connect, 'deliverysystem_couriers',log )
        apiclient.read_last_ts()
        for d in load_queue:
            for c in d:
                print(c)
                print('Сохранение объекта')
                dataLoader.save_object(c, '_id')
        apiclient.write_last_ts()

    couriers_loader = load_couriers()

    @task()
    def load_deliveries():
        apiclient = APIClient(base_url, dwh_pg_connect, 'deliverysystem_deliveries', headers)
        # fromm = datetime.strptime(apiclient.read_last_ts(), '%Y-%m-%dT%H:%M:%S')
        fromm = apiclient.read_last_ts()
        # fromm = fromm.strftime('%Y-%m-%d %H:%M:%S')
        to = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print('Производим загрузку deliveries c {} по {}'.format(fromm,to))
        params_deliveries = {'sort_field': 'id,date','sort_direction': 'asc', 'limit' : 50, 'offset' : 0, 'from': fromm, 'to': to }
        load_queue = apiclient.get('deliveries', params_deliveries)
        print('Данные для загрузки: ', load_queue)
        dataLoader = APIDataLoader(apiclient, dwh_pg_connect, 'deliverysystem_deliveries',log) #ЗДЕСЬ ВЫЗВАЕМ ЗАГРУЗКУ ДАННЫХ ЧЕРЕЗ АПИ
        if len(load_queue) != 0:
            for d in load_queue:
                for c in d:
                    print(c)
                    print('Сохранение объекта')
                    dataLoader.save_object(c, 'order_id')

            last_date = max([parse(i['delivery_ts']).strftime('%Y-%m-%d %H:%M:%S') for t in load_queue[-100:] for i in t ])
            apiclient.write_last_ts(last_date)

    deliveries_loader = load_deliveries()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
       # type: ignore

    [couriers_loader, deliveries_loader]

api_stg_loader = sprint5_project_api_loader()  # noqa