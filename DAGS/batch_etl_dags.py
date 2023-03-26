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
        airbnb_dir = f'{ddir}/Airbnb_Lisbon.csv'
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
        airbnb_lisbon_df = pd.read_csv(airbnb_file)

        ## Cleaning, Extract columns, Merging
        # Table
        airbnb_lisbon_eda = airbnb_lisbon_df.copy()

        # Cleaning Data
        airbnb_lisbon_eda.drop(['borough', 'neighborhood', 'reviews', 'last_modified'], axis = 1, inplace = True)
        airbnb_lisbon_eda['room_type'] = airbnb_lisbon_eda['room_type'].astype('str') 
        airbnb_lisbon_eda.drop(index = [row for row in airbnb_lisbon_eda.index if 0 >= airbnb_lisbon_eda.loc[row, 'accommodates']], inplace = True)
        airbnb_lisbon_eda.drop(index = [row for row in airbnb_lisbon_eda.index if 0 >= airbnb_lisbon_eda.loc[row, 'price']], inplace = True)
        airbnb_lisbon_eda.drop(index = [row for row in airbnb_lisbon_eda.index if 0 >= airbnb_lisbon_eda.loc[row, 'minstay']], inplace = True)

        # Further Cleaning Data for Accommodates
        airbnb_lisbon_eda.drop(index = [row for row in airbnb_lisbon_eda.index if 10 < airbnb_lisbon_eda.loc[row, 'accommodates']], inplace = True)

        # Further Cleaning Data for Bedrooms
        airbnb_lisbon_eda.drop(index = [row for row in airbnb_lisbon_eda.index if 5 < airbnb_lisbon_eda.loc[row, 'bedrooms']], inplace = True)

        # Further Cleaning Data for Minstay
        airbnb_lisbon_eda.drop(index = [row for row in airbnb_lisbon_eda.index if 9 < airbnb_lisbon_eda.loc[row, 'minstay']], inplace = True)

        # Further Cleaning Data for Minstay
        airbnb_lisbon_eda.drop(index = [row for row in airbnb_lisbon_eda.index if 2000 < airbnb_lisbon_eda.loc[row, 'price']], inplace = True)

        ti.xcom_push('airbnb_lisbon_eda', airbnb_lisbon_eda.to_csv())


    def load_hotel(**kwargs):
        ti = kwargs['ti']

        # Create a client object
        client = bigquery.Client()

        hotel_booking_eda_csv = ti.xcom_pull(task_ids = 'transform_hotel', key = 'hotel_booking_eda')
        hotel_booking_cancel_csv = ti.xcom_pull(task_ids = 'transform_hotel', key = 'hotel_booking_eda')

        table_id_h_eda = 'is3107-g2-381308.hotel_bookings.booking_city'
        table_id_h_cancel = 'is3107-g2-381308.hotel_bookings.booking_city'

        new_schema = [
            bigquery.SchemaField('is_canceled', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('lead_time', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_year', 'STRING', mode='NULLABLE')
        ]

        table = client.get_table(table_id)
        
        original_schema = table.schema
        new_schema = original_schema[:] 
        new_schema.extend(new_schema)
        table.schema = new_schema

        table = client.update_table(table, ['schema'])

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, 
            skip_leading_rows=1, 
            autodetect= True, 
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        job = client.load_table_from_dataframe(hotel_city_df, table_id, job_config=job_config)
        job.result()


    def load_airbnb(**kwargs):
        ti = kwargs['ti']

        # Create a client object
        client = bigquery.Client()
        
        airbnb_lisbon_eda_csv = ti.xcom_pull(task_ids = 'transform_airbnb', key = 'airbnb_lisbon_eda')

        table_id_a_eda = 'is3107-g2-381308.airbnb.airbnb_lisbon'

        updated_schema = [
            bigquery.SchemaField('room_id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('host_id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('room_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('overall_satisfaction', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('accommodates', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('bedrooms', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('price', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('minstay', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('latitude', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('longitude', 'FLOAT', mode='NULLABLE'),
        ]

        table = client.get_table(table_id_a_eda)
        
        # original_schema = table.schema
        # new_schema = original_schema[:] 
        # new_schema.extend(new_schema)
        table.schema = updated_schema

        table = client.update_table(table, ['schema'])

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, 
            skip_leading_rows=1, 
            autodetect= True, 
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        job = client.load_table_from_dataframe(airbnb_lisbon_eda_csv, table_id_a_eda, job_config=job_config)
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

    load_hotel = PythonOperator(
        task_id = 'load_hotel',
        python_callable = load_hotel,
        dag = dag,
    )

    load_airbnb = PythonOperator(
        task_id = 'load_airbnb',
        python_callable = load_airbnb,
        dag = dag,
    )


    extract >> [ transform_hotel, transform_airbnb ] >> [ load_hotel, load_airbnb]