# general
import pandas as pd
import csv
import threading
import datetime
from dateutil.parser import parse

# kafka
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# airflow
import airflow_client.client
from airflow_client.client.api import dag_run_api
from airflow.api.client.local_client import Client
# initialise airflow client
client = Client(None, None)

# Configure HTTP basic authorization: Basic
configuration = airflow_client.client.Configuration(
    host = "http://localhost:8008/api/v1",
    username = 'admin',
    password = '94rMesU8ZXhkY7GB'
)


# current working directorty
ddir = '/Users/nevanng/IS3107/IS3107_G2'
streaming_data_dir = ddir + '/Dataset/hotel_streaming/'

# The simulated date when this python script is run 
year = 2017
month = 7

# Pushing our batch data to bigQuery
with airflow_client.client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = dag_run_api.DAGRunApi(api_client)
    try:
        # Trigger a new DAG run
        api_response = api_instance.post_dag_run(
            "batch_etl", dag_run_api.DAGRun(dag_run_id=(datetime.datetime.now().strftime( "%m/%d/%Y, %H:%M:%S")), 
                                                logical_date=parse((datetime.datetime.now() - datetime.timedelta(hours=8)).strftime( "%m/%d/%Y, %H:%M:%S" )+ 'Z'),
                                                conf={}, 
                                                )
        )
         
        print(api_response)
    except airflow_client.client.ApiException as e:
        print("Exception when calling DAGRunApi->post_dag_run: %s\n" % e)

#client.trigger_dag('batch_etl', run_id=None)

# STREAMING
jobs = ['2017-7.csv', '2017-8.csv', '2017-9.csv', 'Q32017','2017-10.csv', '2017-11.csv','2017-12.csv', 'Q42017',
    '2018-1.csv','2018-2.csv','2018-3.csv', 'Q12018','2018-4.csv','2018-5.csv','2018-6.csv', 'Q22018','2018-7.csv',
    '2018-8.csv','2018-9.csv', 'Q32018','2018-10.csv', '2018-11.csv','2018-12.csv', 'Q42018']

# Create Kafka topic
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='test'
)
new_topic = NewTopic(name="hotel_bookings_listener")
admin_client.create_topics([new_topic])

# Set up Kafka producer 
# Assuming there is an upstream booking application which produces a message upon succesful user booking 
# In this case, data is extracted from the csv dataset, divided monthly, and sent to consumer every 15 minutes
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

def sendData(period): 
    year = int(period[:4])
    if len(period) > 10:
        month = int(period[5:7])
    else:
        month = int(period[5:6])       
    print(f'Date: {year}-{month}')
    print(period)
    cur_month_data = open(streaming_data_dir + period , 'r' )
    reader = csv.reader(cur_month_data)

    # cur_month_data = pd.read_csv(streaming_data_dir + period)
    # reader = cur_month_data.to_csv(index=False)

    # 'START' message to indicate new month data
    producer.send('hotel_bookings_listener', value='START'.encode('utf-8'))

    for row in reader:
        message = ','.join(row).encode('utf-8')
        producer.send('hotel_bookings_listener', value=message)
    
    # 'END' message to indicate the end of the current month data
    producer.send('hotel_bookings_listener', 'END'.encode('utf-8'))
    if (month == 12):
        year += 1
        month = 1
    else:
        month += 1

# Quarter Job
# At the end of every quarter (3 months),
# 1. Generate quarter dashboard
# 2. Retrain ML model to include the latest quarter data
def quarterTrigger(quarterJob):
    params = {
        'quarter' : int(quarterJob[1]),
        'year' : int(quarterJob[-4:])
    }

    client.trigger_dag('quarterly_etl', run_id=None, conf=params)

    with airflow_client.client.ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = dag_run_api.DAGRunApi(api_client)
        try:
            # Trigger a new DAG run
            api_response = api_instance.post_dag_run(
                "quarterly_etl", dag_run_api.DAGRun(dag_run_id=(datetime.datetime.now().strftime( "%m/%d/%Y, %H:%M:%S")), 
                                                    logical_date=parse((datetime.datetime.now() - datetime.timedelta(hours=8)).strftime( "%m/%d/%Y, %H:%M:%S" )+ 'Z'),
                                                    conf=params, 
                                                    )
            )
            print(api_response)
        except airflow_client.client.ApiException as e:
            print("Exception when calling DAGRunApi->post_dag_run: %s\n" % e)

for period in range(18):
    if (jobs[period][0] == "Q"):
        t = threading.Timer(1200 * (period), quarterTrigger(jobs[period]))
    else:
        t = threading.Timer(1200 * (period), sendData(jobs[period]))
    t.start()

producer.close()