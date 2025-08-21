from airflow import Dataset

dataset_twse_stock = Dataset("psql://twse_stock")
dataset_tpex_stock = Dataset("psql://tpex_stock")