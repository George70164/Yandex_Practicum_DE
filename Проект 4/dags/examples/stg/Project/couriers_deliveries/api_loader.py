import requests
import psycopg2
from lib import PgConnect 
from datetime import datetime
from lib.dict_util import json2str
from logging import Logger
import json

from typing import Dict, List, Tuple, Optional
from examples.stg import EtlSetting, StgEtlSettingsRepository



class APIClient:
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, base_url, pg_dest: PgConnect, tablename: str, headers=None):
        self.base_url = base_url
        self.headers = headers or {}
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.WF_KEY = "stg_{}_to_stg_workflow".format(tablename)


    def read_last_ts(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, 
                                        workflow_key=self.WF_KEY, 
                                        workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).strftime('%Y-%m-%d %H:%M:%S')})
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            last_loaded_ts = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
        return last_loaded_ts
    
    def write_last_ts(self, last_date=False):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if last_date: 
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_date
            else:
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
        
        
    def get(self, endpoint, params=None):
        list_data = []
        while True:
            url = self.base_url + endpoint
            data = requests.get(url, headers=self.headers, params=params)
            print(data.json())
            if len(data.json()) == 0:
                break
            list_data.append(data.json())
            params['offset'] += params['limit']
            
        return list_data


class APIDataLoader:
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, api_origin: APIClient, pg_dest: PgConnect, tablename: str, logger: Logger ):
        self.WF_KEY = "stg_{}_to_stg_workflow".format(tablename)
        self.api_origin = api_origin
        self.pg_dest = pg_dest
        self.tablename = tablename
        self.log = logger


    def save_object(self, val: Dict, id=None):
        print('Значение для вставки до: ', val)
        fetching_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        str_val = json.dumps(val)
        #str_val = json2str(val)
        print('type(str_val)1: ', type(str_val))
        str_val = str(str_val)
        print('type(str_val)2: ', type(str_val))
        tablename = self.tablename
        print('Значение nаблицы куда производиться запись: ', self.tablename)
        print('Значение для вставки после: ', str_val)
        try:  
            with self.pg_dest.connection() as conn:
                with conn.cursor() as cur:
                    print('начало')
                    print('tablename:', tablename)
                    print('str(val[id]): ', type(str(val[id])))
                    print('type(str(str_val)): ', type(str(str_val)))
                    print('type(str(fetching_time)): ', type(str(fetching_time)))
                    query = f"INSERT INTO stg.{tablename}(object_id, object_value, update_ts) VALUES (%s, %s, %s)"
                    values = (val[id], str_val, fetching_time)
                    cur.execute(query, values)
                    # cur.execute(
                    #     """
                    #         INSERT INTO stg.deliverysystem_couriers(object_value)
                    #         VALUES (%(object_value)s);
                    #     """,
                    #     {
                    #         "object_value": value
                    #         # str(str_val), , object_value, update_tsstr(val[id]),
                    #         # str(fetching_time), %s, %sSET
                    #             # object_value = EXCLUDED.object_value,
                    #             # update_ts = EXCLUDED.update_ts; ON CONFLICT (object_id) DO NOTHING
                    #     }
                    # )
                    print('конец')
        except Exception as error:
            print(error)
            
#     def run_copy(self):
#         self.api_origin.get()

        





# for d in load_queue: