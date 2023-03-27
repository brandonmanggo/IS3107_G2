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
        hotel_booking_dir = f'{ddir}/hotel_bookings.csv'
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
        hotel_booking_eda = hotel_booking_eda.drop['agent', 'company', 'distribution_channel', 'market_segment']

        ti.xcom_push('hotel_booking_eda', hotel_booking_eda.to_csv())

        # Table 2 : Hotel Booking ML (include useful columns only)
        hotel_booking_cancel = hotel_booking_df.copy()
        useless_columns = ['arrival_date_year', 'arrival_date_week_number', 'arrival_date_day_of_month']
        hotel_booking_cancel = hotel_booking_cancel.drop(useless_columns, axis = 1, inplace = True)
        
        ti.xcom_push('hotel_booking_cancel', hotel_booking_cancel.to_csv())


    def transform_airbnb(**kwargs):
        ti = kwargs['ti']
        airbnb_file = ti.xcom_pull(task_ids = 'extract', key = 'airbnb_data')
        hotel_booking_df = pd.read_csv(airbnb_file)

        # Table X :

        # Table Y :

    def load_hotel_bookings(**kwargs):
        ti = kwargs['ti']

        # Create a client object
        client = bigquery.Client()
        
        hotel_booking_eda_csv = ti.xcom_pull(task_ids = 'transform_hotel', key = 'hotel_booking_eda')
        hotel_booking_cancel_csv = ti.xcom_pull(task_ids = 'transform_hotel', key = 'hotel_booking_eda')

        table_id_h_eda = 'is3107-g2-381308.hotel_bookings.booking_city'
        table_id_h_cancel = 'is3107-g2-381308.hotel_bookings.booking_city'

        updated_schema = [
            #bigquery.SchemaField('is_canceled', 'STRING', mode='NULLABLE'),
            #bigquery.SchemaField('lead_time', 'STRING', mode='NULLABLE'),
            #bigquery.SchemaField('arrival_date_year', 'STRING', mode='NULLABLE')
            bigquery.SchemaField('arrival_date_month', 'STRING', mode = 'NULLABLE'),
            bigquery.SchemaField('arrival_date_week_number', 'INTEGER', mode = 'NULLABLE'),
            bigquery.SchemaField('arrival_date_day_of_month', 'INTEGER', mode = 'NULLABLE'),
            bigquery.SchemaField('stays_in_weekend_nights', 'INTEGER', mode = 'NULLABLE'),
            bigquery.SchemaField('stays_in_week_nights', 'INTEGER', mode = 'NULLABLE'),
            bigquery.SchemaField('adults', 'INTEGER', mode = 'NULLABLE'),
            bigquery.SchemaField('children', 'INTEGER', mode = 'NULLABLE'),
            bigquery.SchemaField('babies', 'INTEGER', mode = 'NULLABLE'),
            bigquery.SchemaField('meal', 'STRING', mode = 'NULLABLE'),
            bigquery.SchemaField('country', 'STRING', mode = 'NULLABLE'),
            bigquery.SchemaField('is_repeated_guest', 'STRING', mode = 'NULLABLE'), #Categorical
            bigquery.SchemaField('previous_cancellations', 'INTEGER', mode = 'NULLABLE'),
            bigquery.SchemaField('previous_bookings_not_cancelled', 'INTEGER', mode = 'NULLABLE'),
            bigquery.SchemaField('reserved_room_type', 'STRING', mode = 'NULLABLE'), #Categorical
            bigquery.SchemaField('assigned_room_type', 'STRING', mode = 'NULLABLE'), #Categorical
            bigquery.SchemaField('booking_changes', 'INTEGER', mode = 'NULLABLE'),
            bigquery.SchemaField('deposit_type', 'STRING', mode = 'NULLABLE'), #Categorical
            bigquery.SchemaField('days_in_waiting_list', 'INTEGER', mode = 'NULLABLE'),
            bigquery.SchemaField('customer_type', 'STRING', mode = 'NULLABLE'),
            bigquery.SchemaField('adr', 'FLOAT', mode = 'NULLABLE'),
            bigquery.SchemaField('required_car_parking_spaces', 'INTEGER', mode = 'NULLABLE'),
            bigquery.SchemaField('total_of_special_requests', 'INTEGER', mode = 'NULLABLE'),
            bigquery.SchemaField('reservation_status', 'STRING', mode = 'NULLABLE'), #Categorical
            bigquery.SchemaField('reservation_status_date', 'DATE', mode = 'NULLABLE')
        ]

        # Get the table object
        table = client.get_table(table_id)

        # create schema for table (just run this once)
        # comment this out if your schema is defined
        #original_schema = table.schema
        #new_schema = original_schema[:] 
        #new_schema.extend(updated_schema)
        table.schema = updated_schema
        table = client.update_table(table, ['schema'])
        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV, 
                                            skip_leading_rows=1, 
                                            autodetect= True,
                                            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        job = client.load_table_from_file(hotel_booking_eda_csv, table_id_h_eda, job_config=job_config)
        job.result()


        



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