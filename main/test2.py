# general
import pandas as pd
import csv
import threading

# kafka
from kafka import KafkaProducer

# airflow
import datetime
from dateutil.parser import parse
import airflow_client.client
from airflow_client.client.api import dag_run_api
from airflow_client.client.model.dag_run import DAGRun
from airflow_client.client.model.dag_state import DagState
from airflow_client.client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.
configuration = airflow_client.client.Configuration(
    host = "http://localhost:8080/api/v1"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: Basic
configuration = airflow_client.client.Configuration(
    host = "http://localhost:8080/api/v1",
    username = 'admin',
    password = '94rMesU8ZXhkY7GB'
)

reader = pd.read_csv("/Users/nevanng/IS3107/IS3107_G2/Dataset/hotel_streaming/2017-7.csv")
reader = reader.to_csv(index=False)
# Enter a context with an instance of the API client
with airflow_client.client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = dag_run_api.DAGRunApi(api_client)
    dag_id = "streaming_etl" # str | The DAG ID.
    
    # example passing only required values which don't have defaults set
    try:
        # Trigger a new DAG run
        #api_response = api_instance.post_dag_run(dag_id, dag_run)
        # api_response = api_instance.post_dag_run(
        #     "streaming_etl", dag_run_api.DAGRun(
        #                                         dag_run_id=(datetime.datetime.now().strftime( "%m/%d/%Y, %H:%M:%S")), 
        #                                         logical_date=parse((datetime.datetime.now() - datetime.timedelta(hours=8)).strftime( "%m/%d/%Y, %H:%M:%S" )+ 'Z'),
        #                                         conf={'data' : reader}, 
        #                                         )
        # )
        api_response = api_instance.post_dag_run(
            "quarterly_etl", dag_run_api.DAGRun(
                                                dag_run_id=(datetime.datetime.now().strftime( "%m/%d/%Y, %H:%M:%S")), 
                                                logical_date=parse((datetime.datetime.now() - datetime.timedelta(hours=8)).strftime( "%m/%d/%Y, %H:%M:%S" )+ 'Z'),
                                                conf={}, 
                                                )
        ) 
        pprint(api_response)
    except airflow_client.client.ApiException as e:
        print("Exception when calling DAGRunApi->post_dag_run: %s\n" % e)

# from airflow.api.client.local_client import Client
# import time
# import airflow_client.client
# from pprint import pprint
# from airflow_client.client.api import config_api
# from airflow_client.client.model.config import Config
# from airflow_client.client.model.error import Error
# initialise airflow client

# Configure HTTP basic authorization: Basic

# # current working directorty
# ddir = '/IS3107/IS3107_G2'
# streaming_data_dir = ddir + '/Dataset/hotel_streaming/2017-10.csv'

# #cur_month_data = open("/Users/nevanng/IS3107/IS3107_G2/Dataset/hotel_streaming/2017-10.csv", 'r' )
# #reader = csv.reader(cur_month_data)

# reader = pd.read_csv("/Users/nevanng/IS3107/IS3107_G2/Dataset/hotel_streaming/2017-7.csv")
# reader = reader.to_csv(index=False)
# client.trigger_dag('streaming_etl', run_id=None, conf={'data' : reader})
