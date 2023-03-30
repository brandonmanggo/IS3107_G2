from kafka import KafkaConsumer
import io
import csv

# airflow
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
        client.trigger_dag('streaming_etl', run_id=None, conf={'data' : cur_month_csv})
    # Write CSV row 
    else:
        csv.writer(message_str.split(','))


