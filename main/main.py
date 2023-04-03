# general
import csv
import threading

# kafka
from kafka import KafkaProducer

# airflow
from airflow.api.client.local_client import Client
# initialise airflow client
client = Client(None, None)

# current working directorty
ddir = '/Users/mellitaangga/Desktop/BZA/Y2S2/IS3107/Project/IS3107_G2'
streaming_data_dir = ddir + '/Dataset/hotel_streaming'

# The simulated date when this python script is run 
year = 2017
month = 7

# Pushing our batch data to bigQuery
client.trigger_dag('batch_etl', run_id=None)

# STREAMING
jobs = ['2017-7.csv', '2017-8.csv', '2017-9.csv', 'Q32017','2017-10.csv', '2017-11.csv','2017-12.csv', 'Q42017',
    '2018-1.csv','2018-2.csv','2018-3.csv', 'Q12018','2018-4.csv','2018-5.csv','2018-6.csv', 'Q22018','2018-7.csv',
    '2018-8.csv','2018-9.csv', 'Q32018','2018-10.csv', '2018-11.csv','2018-12.csv', 'Q42018']

# Set up Kafka producer 
# Assuming there is an upstream booking application which produces a message upon succesful user booking 
# In this case, data is extracted from the csv dataset, divided monthly, and sent to consumer every 15 minutes
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

def sendData(period): 
    print(f'Date: {year}-{month}')
    cur_month_data = open(streaming_data_dir + period , 'r' )
    reader = csv.reader(cur_month_data)

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
        'year' : int(quarter[-4:])
    }

    client.trigger_dag('quarterly_dag', run_id=None, conf=params)

for period in range(18):
    if (jobs[period][0] == "Q"):
        t = threading.Timer(1200 * (period), quarterTrigger(jobs[period]))
    else:
        t = threading.Timer(1200 * (period), sendData(jobs[period]))
    t.start()

producer.close()