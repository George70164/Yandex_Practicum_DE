import requests
import json
from logging import Logger


log = Logger

try:
    print(1+"a")
except Exception as e:
    log.warning(e)






url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers'
restaurants_parameters = {'limit': '10',  'offset': '0'}
request_headers = { 'X-Nickname': 'Gera', 'X-Cohort': '22', 'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'}
rs = requests.get(url, headers = request_headers, params = restaurants_parameters)

#print('status_code = ', rs.status_code) 
#print(rs.content) 
r = [print(courier) for courier in json.loads(rs.content)]



