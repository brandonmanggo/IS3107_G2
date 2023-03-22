import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from google.cloud import bigquery
import pandas as pd
import numpy as np

# Set the path to your service account key file
# Change the dir according to the location of the service account credential (is3107-g2-381308-b948b933d07a.json)
ddir = '/Users/mellitaangga/Desktop/BZA/Y2S2/IS3107/Project'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{ddir}/is3107-g2-381308-b948b933d07a.json'


default_args = {
    'owner': 'airflow', 
}

with DAG(
    "batch_processing_hotel",
    default_args=default_args,
    description='ETL Hotel Booking Datasets',
    schedule_interval=None,
    # start_date=datetime(2023, 3, 13),
    catchup=False,
    tags=['example'],
) as dag:
    
    def extract(**kwargs):
        ti = kwargs['ti']

        # Hotel Booking Dataset
        hotel_booking_dir = f'{ddir}/Hotel_Reservations.csv'
        hotel_booking_file = open(hotel_booking_dir).read()

        # Airbnb Dataset
        airbnb_dir = f'{ddir}/Hotel_Reservations.csv'
        airbnb_file = open(airbnb_dir).read()

        ti.xcom_push('hotel_booking_data', hotel_booking_file)
        ti.xcom_push('airbnb_data', airbnb_file)



    def transform_hotel(**kwargs):
        ti = kwargs['ti']
        hotel_booking_file = ti.xcom_pull(task_ids = 'extract', key = 'hotel_booking_data')
        hotel_booking_df = pd.read_csv(hotel_booking_file)

        # Cleaning, extract columns, merging

        # Table 1 : Hotel Booking EDA (include most columns)
        hotel_booking_eda = hotel_booking_df.copy()

        ti.xcom_push('hotel_booking_eda', hotel_booking_eda.to_csv())

        # Table 2 : Hotel Booking ML (include useful columns only)
        hotel_booking_cancel = hotel_booking_df.copy()

        ti.xcom_push('hotel_booking_cancel', hotel_booking_cancel.to_csv())


    def transform_airbnb(**kwargs):
        ti = kwargs['ti']
        airbnb_file = ti.xcom_pull(task_ids = 'extract', key = 'airbnb_data')
        hotel_booking_df = pd.read_csv(airbnb_file)

        # Table X :

        # Table Y :

    def load(**kwargs):
        ti = kwargs['ti']

        hotel_booking_eda_csv = ti.xcom_pull(task_ids = 'transform_hotel', key = 'hotel_booking_eda')
        hotel_booking_cancel_csv = ti.xcom_pull(task_ids = 'transform_hotel', key = 'hotel_booking_eda')

        # Create a client object
        client = bigquery.Client()

        table_id_h_eda = 'is3107-g2-381308.hotel_bookings.booking_city'
        table_id_h_cancel = 'is3107-g2-381308.hotel_bookings.booking_city'
        table_id_a_eda = 'is3107-g2-381308.hotel_bookings.booking_city'


        new_schema = [
            bigquery.SchemaField('is_canceled', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('lead_time', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_year', 'STRING', mode='NULLABLE')
        ]

        table = client.get_table(table_id)
        original_schema = table.schema
        new_schema = original_schema[:] 

        new_schema.extend(neww_schema)

        table.schema = new_schema

        table = client.update_table(table, ['schema'])

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect= True, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        job = client.load_table_from_dataframe(hotel_city_df, table_id, job_config=job_config)
        job.result()



    extract = PythonOperator(
        task_id = 'extract',
        python_callable = extract,
        dag = dag,
    )

    transform_hotel = PythonOperator(
        task_id = 'transform_hotel',
        python_callable = transform_hotel,
        dag = dag,
    )

    transform_airbnb = PythonOperator(
        task_id = 'transform_airbnb',
        python_callable = transform_airbnb,
        dag = dag,
    )

    load = PythonOperator(
        task_id = 'load',
        python_callable = load,
        dag = dag,
    )


    extract >> [ transform_hotel, transform_airbnb ] >> load