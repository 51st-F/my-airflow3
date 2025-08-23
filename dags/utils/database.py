
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017')
db_openapi_twse = client['openapi_twse']
db_openapi_tpex = client['openapi_tpex']
db_openapi_taifex = client['openapi_taifex']