from kafka import KafkaConsumer
import io
import csv

# airflow
import airflow_client.client
from airflow_client.client.api import dag_run_api
from airflow.api.client.local_client import Client
# initialise airflow client
client = Client(None, None)

# Set up Kafka consumer
consumer = KafkaConsumer('test', bootstrap_servers=['localhost:9092'])

# Create new csv file 
cur_month_csv = io.StringIO()
csv_writer = csv.writer(cur_month_csv)

# Continuously poll for new messages
for message in consumer:
    # Decode the message bytes to a string
    message_str = message.value.decode('utf-8')
    # on 'START', create empty csv object
    if (message_str == "START"):
        cur_month_csv = io.StringIO()
        csv_writer = csv.writer(cur_month_csv)
    # on 'END', trigger DAGS for streaming ETL
    elif (message_str == "END"):
        with airflow_client.client.ApiClient(configuration) as api_client:
            # Create an instance of the API class
            api_instance = dag_run_api.DAGRunApi(api_client)
            try:
                # Trigger a new DAG run
                api_response = api_instance.post_dag_run(
                    "streaming_etl", dag_run_api.DAGRun(dag_run_id=(datetime.datetime.now().strftime( "%m/%d/%Y, %H:%M:%S")), 
                                                        logical_date=parse((datetime.datetime.now() - datetime.timedelta(hours=8)).strftime( "%m/%d/%Y, %H:%M:%S" )+ 'Z'),
                                                        conf={'data' : cur_month_csv}, 
                                                        )
                )
            except airflow_client.client.ApiException as e:
                print("Exception when calling DAGRunApi->post_dag_run: %s\n" % e)
    # Write CSV row 
    else:
        csv.writer(message_str.split(','))


