# general
import csv
import threading
import pandas as pd

# kafka
from kafka import KafkaProducer

# airflow
from airflow.api.client.local_client import Client
# initialise airflow client
client = Client(None, None)
# current working directorty
ddir = '/IS3107/IS3107_G2'
streaming_data_dir = ddir + '/Dataset/hotel_streaming/2017-10.csv'

cur_month_data = pd.read_csv("/Users/nevanng/IS3107/IS3107_G2/Dataset/hotel_streaming/2017-7.csv")
reader = cur_month_data.to_csv(index=False)
client.trigger_dag('streaming_etl', run_id=None, conf={'data' : reader})