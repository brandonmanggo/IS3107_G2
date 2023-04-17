import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from google.cloud import bigquery
import pandas as pd
import numpy as np
import io
import csv
from io import StringIO
# Set the path to your service account key file
# Change the dir according to the location of the service account credential (is3107-g2-381308-b948b933d07a.json)
ddir = '/Users/nevanng/IS3107/IS3107_G2/'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{ddir}/BigQuery/is3107-g2-381308-b948b933d07a.json'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 26),
}

with DAG(
    "batch_etl",
    default_args=default_args,
    description='Batch Data ETL for Hotel and Airbnb Datasets',
    schedule_interval=None,
    # start_date=datetime(2023, 3, 13),
    catchup=False,
    tags=['example'],
) as dag:
    
    def extract(**kwargs):
        ti = kwargs['ti']

        # Hotel Booking Dataset
        hotel_booking_file = pd.read_csv(f'{ddir}/Dataset/batch_data/city_hotel_bookings_updated.csv')
        hotel_booking_file = hotel_booking_file.to_csv(index=False)
       # hotel_booking_dir = f'{ddir}/Dataset/batch_data/city_hotel_bookings_updated.csv'
       # hotel_booking_file = open(hotel_booking_dir)
       # hotel_booking_file = csv.reader(hotel_booking_file)
        

        # Airbnb Dataset
        airbnb_file = pd.read_csv(f'{ddir}/Dataset/batch_data/data_airbnb_raw.csv') 
        airbnb_file = airbnb_file.to_csv(index=False)
        #airbnb_dir = f'{ddir}/Dataset/batch_data/data_airbnb_raw.csv'
        #airbnb_file = open(airbnb_dir)
        #airbnb_file = csv.reader(airbnb_file)

        #print(airbnb_file)
        ti.xcom_push('hotel_booking_raw_data', hotel_booking_file)
        ti.xcom_push('airbnb_raw_data', airbnb_file)


    def transform_hotel(**kwargs):
        ti = kwargs['ti']
        hotel_booking_file = ti.xcom_pull(task_ids = 'extract', key = 'hotel_booking_raw_data')
        hotel_booking_df = pd.read_csv(StringIO(hotel_booking_file))
        

        # Cleaning, extract columns, merging

        # Table 1 : Hotel Booking records (include all columns) (raw dataset for EDA)
        hotel_booking_eda = hotel_booking_df.copy()
        
        hotel_booking_eda = hotel_booking_eda

        ti.xcom_push('hotel_booking_eda', hotel_booking_eda.to_csv(index=False))

        # Table 2 : Hotel Booking ML Cancellation (include useful columns only)
        hotel_booking_ml_cancel = hotel_booking_df.copy()

        ml_cancel_included_cols = ['Booking_ID', 'adults', 'children', 'stays_in_weekend_nights',
                                    'stays_in_week_nights', 'meal', 'required_car_parking_spaces',
                                    'reserved_room_type', 'lead_time', 'arrival_date_year',
                                    'arrival_date_month', 'arrival_date_day_of_month', 'market_segment',
                                    'is_repeated_guest', 'previous_cancellations',
                                    'previous_bookings_not_canceled', 'adr', 'total_of_special_requests',
                                    'is_canceled']

        hotel_booking_ml_cancel = hotel_booking_ml_cancel[ml_cancel_included_cols]

        ## One-Hot Encoding
        # For market_segment Column
        market_segment_one_hot = pd.get_dummies(hotel_booking_ml_cancel['market_segment'], prefix='market_segment')
        hotel_booking_ml_cancel = pd.concat([hotel_booking_ml_cancel, market_segment_one_hot], axis=1)
        hotel_booking_ml_cancel.drop('market_segment', axis=1, inplace=True)

        # For arrival_date_month Column
        arrival_date_month_one_hot = pd.get_dummies(hotel_booking_ml_cancel['arrival_date_month'], prefix='arrival_date_month')
        hotel_booking_ml_cancel = pd.concat([hotel_booking_ml_cancel, arrival_date_month_one_hot], axis=1)
        hotel_booking_ml_cancel.drop('arrival_date_month', axis=1, inplace=True)

        # For meal Column
        meal_one_hot = pd.get_dummies(hotel_booking_ml_cancel['meal'], prefix='meal')
        hotel_booking_ml_cancel = pd.concat([hotel_booking_ml_cancel, meal_one_hot], axis=1)
        hotel_booking_ml_cancel.drop('meal', axis=1, inplace=True)

        # For reserved_room_type Column
        reserved_room_type_one_hot = pd.get_dummies(hotel_booking_ml_cancel['reserved_room_type'], prefix='reserved_room_type')
        hotel_booking_ml_cancel = pd.concat([hotel_booking_ml_cancel, reserved_room_type_one_hot], axis=1)
        hotel_booking_ml_cancel.drop('reserved_room_type', axis=1, inplace=True)

        hotel_booking_ml_cancel['predicted'] = pd.Series([0] * len(hotel_booking_ml_cancel))

        #hotel_booking_ml_cancel['Booking_ID'] = hotel_booking_ml_cancel['Booking_ID'].str[3:]
        #hotel_booking_ml_cancel['Booking_ID'] = hotel_booking_ml_cancel['Booking_ID'].astype('int') 
        hotel_booking_ml_cancel['market_segment_Aviation'] = hotel_booking_ml_cancel['market_segment_Aviation'].astype('int') 
        hotel_booking_ml_cancel['market_segment_Complementary'] = hotel_booking_ml_cancel['market_segment_Complementary'].astype('int') 
        hotel_booking_ml_cancel['market_segment_Corporate'] = hotel_booking_ml_cancel['market_segment_Corporate'].astype('int')
        hotel_booking_ml_cancel['market_segment_Direct'] = hotel_booking_ml_cancel['market_segment_Direct'].astype('int')
        hotel_booking_ml_cancel['market_segment_Groups'] = hotel_booking_ml_cancel['market_segment_Groups'].astype('int')
        hotel_booking_ml_cancel['market_segment_Offline TA/TO'] = hotel_booking_ml_cancel['market_segment_Offline TA/TO'].astype('int')
        hotel_booking_ml_cancel['market_segment_Online TA'] = hotel_booking_ml_cancel['market_segment_Online TA'].astype('int')
        hotel_booking_ml_cancel['market_segment_Undefined'] = hotel_booking_ml_cancel['market_segment_Undefined'].astype('int')
        hotel_booking_ml_cancel['arrival_date_month_April'] = hotel_booking_ml_cancel['arrival_date_month_April'].astype('int')
        hotel_booking_ml_cancel['arrival_date_month_August'] = hotel_booking_ml_cancel['arrival_date_month_August'].astype('int')
        hotel_booking_ml_cancel['arrival_date_month_December'] = hotel_booking_ml_cancel['arrival_date_month_December'].astype('int')
        hotel_booking_ml_cancel['arrival_date_month_February'] = hotel_booking_ml_cancel['arrival_date_month_February'].astype('int')
        hotel_booking_ml_cancel['arrival_date_month_January'] = hotel_booking_ml_cancel['arrival_date_month_January'].astype('int')
        hotel_booking_ml_cancel['arrival_date_month_July'] = hotel_booking_ml_cancel['arrival_date_month_July'].astype('int')
        hotel_booking_ml_cancel['arrival_date_month_June'] = hotel_booking_ml_cancel['arrival_date_month_June'].astype('int')
        hotel_booking_ml_cancel['arrival_date_month_March'] = hotel_booking_ml_cancel['arrival_date_month_March'].astype('int')
        hotel_booking_ml_cancel['arrival_date_month_May'] = hotel_booking_ml_cancel['arrival_date_month_May'].astype('int')
        hotel_booking_ml_cancel['arrival_date_month_November'] = hotel_booking_ml_cancel['arrival_date_month_November'].astype('int')
        hotel_booking_ml_cancel['arrival_date_month_October'] = hotel_booking_ml_cancel['arrival_date_month_October'].astype('int')
        hotel_booking_ml_cancel['arrival_date_month_September'] = hotel_booking_ml_cancel['arrival_date_month_September'].astype('int')
        hotel_booking_ml_cancel['meal_BB'] = hotel_booking_ml_cancel['meal_BB'].astype('int')
        hotel_booking_ml_cancel['meal_FB'] = hotel_booking_ml_cancel['meal_FB'].astype('int')
        hotel_booking_ml_cancel['meal_HB'] = hotel_booking_ml_cancel['meal_HB'].astype('int')
        hotel_booking_ml_cancel['meal_SC'] = hotel_booking_ml_cancel['meal_SC'].astype('int')
        hotel_booking_ml_cancel['reserved_room_type_A'] = hotel_booking_ml_cancel['reserved_room_type_A'].astype('int')
        hotel_booking_ml_cancel['reserved_room_type_B'] = hotel_booking_ml_cancel['reserved_room_type_B'].astype('int')
        hotel_booking_ml_cancel['reserved_room_type_C'] = hotel_booking_ml_cancel['reserved_room_type_C'].astype('int')
        hotel_booking_ml_cancel['reserved_room_type_D'] = hotel_booking_ml_cancel['reserved_room_type_D'].astype('int')
        hotel_booking_ml_cancel['reserved_room_type_E'] = hotel_booking_ml_cancel['reserved_room_type_E'].astype('int')
        hotel_booking_ml_cancel['reserved_room_type_F'] = hotel_booking_ml_cancel['reserved_room_type_F'].astype('int')
        hotel_booking_ml_cancel['reserved_room_type_G'] = hotel_booking_ml_cancel['reserved_room_type_G'].astype('int')
        
        ti.xcom_push('hotel_booking_ml_cancel', hotel_booking_ml_cancel.to_csv(index=False))

        # Table 3 : Hotel Booking ML Price Prediction (include useful columns only)
        hotel_booking_ml_price = hotel_booking_df.copy()

        ml_price_included_cols = ['Booking_ID','adults', 'children', 'stays_in_weekend_nights',
                                  'stays_in_week_nights', 'meal', 'required_car_parking_spaces',
                                  'reserved_room_type', 'lead_time', 'arrival_date_year',
                                  'arrival_date_month', 'arrival_date_day_of_month', 
                                  'market_segment', 'is_repeated_guest', 'previous_cancellations',
                                  'previous_bookings_not_canceled', 'adr', 'total_of_special_requests']

        hotel_booking_ml_price = hotel_booking_ml_price[ml_price_included_cols]

        ## Pre-Processing

        # Filling Missing Data for Children
        hotel_booking_ml_price.children.fillna(0, inplace = True)
        hotel_booking_ml_price['children'] = hotel_booking_ml_price['children'].astype('int') 

        ## Adjust to suit Streaming Data - Hotel Reservation
        hotel_booking_ml_price.loc[(hotel_booking_ml_price['meal'] == 'Undefined'), 'meal']= 'SC'
        hotel_booking_ml_price.loc[(hotel_booking_ml_price['required_car_parking_spaces'] > 1), 'required_car_parking_spaces']= 1

        # Cleaning Data for adr
        hotel_booking_ml_price.drop(index = [row for row in hotel_booking_ml_price.index 
                                            if 400 < hotel_booking_ml_price.loc[row, 'adr']], 
                                    inplace = True)
        hotel_booking_ml_price.drop(index = [row for row in hotel_booking_ml_price.index 
                                            if 0 >= hotel_booking_ml_price.loc[row, 'adr']], 
                                    inplace = True)
        hotel_booking_ml_price.adr = hotel_booking_ml_price.adr.round()
        hotel_booking_ml_price.adr = hotel_booking_ml_price.adr.astype('int')

        ## One-Hot Encoding
        # For market_segment Column
        market_segment_one_hot = pd.get_dummies(hotel_booking_ml_price['market_segment'], prefix='market_segment')
        hotel_booking_ml_price = pd.concat([hotel_booking_ml_price, market_segment_one_hot], axis=1)
        hotel_booking_ml_price.drop('market_segment', axis=1, inplace=True)

        # For arrival_date_month Column
        arrival_date_month_one_hot = pd.get_dummies(hotel_booking_ml_price['arrival_date_month'], prefix='arrival_date_month')
        hotel_booking_ml_price = pd.concat([hotel_booking_ml_price, arrival_date_month_one_hot], axis=1)
        hotel_booking_ml_price.drop('arrival_date_month', axis=1, inplace=True)

        # For meal Column
        meal_one_hot = pd.get_dummies(hotel_booking_ml_price['meal'], prefix='meal')
        hotel_booking_ml_price = pd.concat([hotel_booking_ml_price, meal_one_hot], axis=1)
        hotel_booking_ml_price.drop('meal', axis=1, inplace=True)

        # For reserved_room_type Column
        reserved_room_type_one_hot = pd.get_dummies(hotel_booking_ml_price['reserved_room_type'], prefix='reserved_room_type')
        hotel_booking_ml_price = pd.concat([hotel_booking_ml_price, reserved_room_type_one_hot], axis=1)
        hotel_booking_ml_price.drop('reserved_room_type', axis=1, inplace=True)

        hotel_booking_ml_price['predicted'] = pd.Series([0] * len(hotel_booking_ml_price))
        
        #hotel_booking_ml_price['Booking_ID'] = hotel_booking_ml_price['Booking_ID'].str[3:]
        #hotel_booking_ml_price['Booking_ID'] = hotel_booking_ml_price['Booking_ID'].astype('int') 
        hotel_booking_ml_price['market_segment_Aviation'] = hotel_booking_ml_price['market_segment_Aviation'].astype('int') 
        hotel_booking_ml_price['market_segment_Complementary'] = hotel_booking_ml_price['market_segment_Complementary'].astype('int') 
        hotel_booking_ml_price['market_segment_Corporate'] = hotel_booking_ml_price['market_segment_Corporate'].astype('int')
        hotel_booking_ml_price['market_segment_Direct'] = hotel_booking_ml_price['market_segment_Direct'].astype('int')
        hotel_booking_ml_price['market_segment_Groups'] = hotel_booking_ml_price['market_segment_Groups'].astype('int')
        hotel_booking_ml_price['market_segment_Offline TA/TO'] = hotel_booking_ml_price['market_segment_Offline TA/TO'].astype('int')
        hotel_booking_ml_price['market_segment_Online TA'] = hotel_booking_ml_price['market_segment_Online TA'].astype('int')
        hotel_booking_ml_price['market_segment_Undefined'] = hotel_booking_ml_price['market_segment_Undefined'].astype('int')
        hotel_booking_ml_price['arrival_date_month_April'] = hotel_booking_ml_price['arrival_date_month_April'].astype('int')
        hotel_booking_ml_price['arrival_date_month_August'] = hotel_booking_ml_price['arrival_date_month_August'].astype('int')
        hotel_booking_ml_price['arrival_date_month_December'] = hotel_booking_ml_price['arrival_date_month_December'].astype('int')
        hotel_booking_ml_price['arrival_date_month_February'] = hotel_booking_ml_price['arrival_date_month_February'].astype('int')
        hotel_booking_ml_price['arrival_date_month_January'] = hotel_booking_ml_price['arrival_date_month_January'].astype('int')
        hotel_booking_ml_price['arrival_date_month_July'] = hotel_booking_ml_price['arrival_date_month_July'].astype('int')
        hotel_booking_ml_price['arrival_date_month_June'] = hotel_booking_ml_price['arrival_date_month_June'].astype('int')
        hotel_booking_ml_price['arrival_date_month_March'] = hotel_booking_ml_price['arrival_date_month_March'].astype('int')
        hotel_booking_ml_price['arrival_date_month_May'] = hotel_booking_ml_price['arrival_date_month_May'].astype('int')
        hotel_booking_ml_price['arrival_date_month_November'] = hotel_booking_ml_price['arrival_date_month_November'].astype('int')
        hotel_booking_ml_price['arrival_date_month_October'] = hotel_booking_ml_price['arrival_date_month_October'].astype('int')
        hotel_booking_ml_price['arrival_date_month_September'] = hotel_booking_ml_price['arrival_date_month_September'].astype('int')
        hotel_booking_ml_price['meal_BB'] = hotel_booking_ml_price['meal_BB'].astype('int')
        hotel_booking_ml_price['meal_FB'] = hotel_booking_ml_price['meal_FB'].astype('int')
        hotel_booking_ml_price['meal_HB'] = hotel_booking_ml_price['meal_HB'].astype('int')
        hotel_booking_ml_price['meal_SC'] = hotel_booking_ml_price['meal_SC'].astype('int')
        hotel_booking_ml_price['reserved_room_type_A'] = hotel_booking_ml_price['reserved_room_type_A'].astype('int')
        hotel_booking_ml_price['reserved_room_type_B'] = hotel_booking_ml_price['reserved_room_type_B'].astype('int')
        hotel_booking_ml_price['reserved_room_type_C'] = hotel_booking_ml_price['reserved_room_type_C'].astype('int')
        hotel_booking_ml_price['reserved_room_type_D'] = hotel_booking_ml_price['reserved_room_type_D'].astype('int')
        hotel_booking_ml_price['reserved_room_type_E'] = hotel_booking_ml_price['reserved_room_type_E'].astype('int')
        hotel_booking_ml_price['reserved_room_type_F'] = hotel_booking_ml_price['reserved_room_type_F'].astype('int')
        hotel_booking_ml_price['reserved_room_type_G'] = hotel_booking_ml_price['reserved_room_type_G'].astype('int')

        ti.xcom_push('hotel_booking_ml_price', hotel_booking_ml_price.to_csv(index=False))


    def transform_airbnb(**kwargs):
        ti = kwargs['ti']
        airbnb_file = ti.xcom_pull(task_ids = 'extract', key = 'airbnb_raw_data')
        airbnb_df = pd.read_csv(StringIO(airbnb_file))

        ## Cleaning, Extract columns, Merging
        # Dataframe
        airbnb_eda = airbnb_df.copy()

        # Further Cleaning Data for Accommodates
        airbnb_eda.drop(index = [row for row in airbnb_eda.index if 10 < airbnb_eda.loc[row, 'accommodates']], inplace = True)

        # Further Cleaning Data for Bedrooms
        airbnb_eda.drop(index = [row for row in airbnb_eda.index if 5 < airbnb_eda.loc[row, 'bedrooms']], inplace = True)

        # Further Cleaning Data for Minstay
        airbnb_eda.drop(index = [row for row in airbnb_eda.index if 5 < airbnb_eda.loc[row, 'minstay']], inplace = True)

        ti.xcom_push('airbnb_eda', airbnb_eda.to_csv(index = False))


    def load_hotel(**kwargs):
        ti = kwargs['ti']

        hotel_booking_eda_csv = ti.xcom_pull(task_ids = 'transform_hotel', key = 'hotel_booking_eda')
        hotel_booking_ml_cancel_csv = ti.xcom_pull(task_ids = 'transform_hotel', key = 'hotel_booking_ml_cancel')
        hotel_booking_ml_price_csv = ti.xcom_pull(task_ids = 'transform_hotel', key = 'hotel_booking_ml_price')

        hotel_booking_eda_csv_bytes = bytes(hotel_booking_eda_csv, 'utf-8')
        hotel_booking_eda_csv_stream = io.BytesIO(hotel_booking_eda_csv_bytes)

        hotel_booking_ml_cancel_csv_bytes = bytes(hotel_booking_ml_cancel_csv, 'utf-8')
        hotel_booking_ml_cancel_csv_stream = io.BytesIO(hotel_booking_ml_cancel_csv_bytes)

        hotel_booking_ml_price_csv_bytes = bytes(hotel_booking_ml_price_csv, 'utf-8')
        hotel_booking_ml_price_csv_stream = io.BytesIO(hotel_booking_ml_price_csv_bytes)

        # Create a client object
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{ddir}/BigQuery/is3107-g2-381308-b948b933d07a.json'
        
        client = bigquery.Client()

        # Set Table ID
        table_id_h_eda = 'is3107-g2-381308.hotel_booking.hotel_booking_eda'
        table_id_h_ml_cancel = 'is3107-g2-381308.hotel_booking.hotel_booking_ml_cancel'
        table_id_h_ml_price = 'is3107-g2-381308.hotel_booking.hotel_booking_ml_price'

        # Define Table Schema
        eda_schema = [
            bigquery.SchemaField('Booking_ID', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('is_canceled', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('lead_time', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_year', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_month', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_week_number', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('arrival_month', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_day_of_month', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('stays_in_weekend_nights', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('stays_in_week_nights', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('adults', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('children', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('babies', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('meal', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('country', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('market_segment', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('distribution_channel', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('is_repeated_guest', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('previous_cancellations', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('previous_bookings_not_canceled', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type ', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('assigned_room_type ', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('booking_changes', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('agent', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('company', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('days_in_waiting_list', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('customer_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('adr', 'FLOAT64', mode='NULLABLE'),
            bigquery.SchemaField('required_car_parking_spaces', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('total_of_special_requests', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('reservation_status', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('reservation_status_date', 'STRING', mode='NULLABLE'),
        ]

        ml_cancel_schema = [
            bigquery.SchemaField('Booking_ID', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('adults', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('children', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('stays_in_weekend_nights', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('stays_in_week_nights', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('meal_BB', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('meal_FB', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('meal_HB', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('meal_SC', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('required_car_parking_spaces', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_A', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_B', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_C', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_D', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_E', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_F', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_G', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_H', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_L', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_P', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('lead_time', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_month_April', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_month_August', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_month_December', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_month_February', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_month_January', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_month_July', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_month_June', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_month_March', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_month_May', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_month_November', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_month_October', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_month_September', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('arrival_date_day_of_month', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('market_segment_Aviation', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('market_segment_Complementary', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('market_segment_Corporate', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('market_segment_Direct', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('market_segment_Groups', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('market_segment_Offline TA_TO', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('market_segment_Online TA', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('market_segment_Undefined', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('is_repeated_guest', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('previous_cancellations', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('previous_bookings_not_canceled', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('adr', 'FLOAT64', mode='NULLABLE'),
            bigquery.SchemaField('total_of_special_requests', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('is_canceled', 'INT64', mode='NULLABLE')
        ]

        ml_price_schema = [
            bigquery.SchemaField('Booking_ID', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('lead_time', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('stays_in_weekend_nights', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('stays_in_week_nights', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('adults', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('children', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('is_repeated_guest', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('previous_cancellations', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('previous_bookings_not_canceled', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('required_car_parking_spaces', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('total_of_special_requests', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('market_segment_Aviation', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('market_segment_Complementary', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('market_segment_Corporate', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('market_segment_Direct', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('market_segment_Groups', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('market_segment_Offline_TA_TO', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('market_segment_Online_TA', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('market_segment_Undefined', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('arrival_month_April', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('arrival_month_August', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('arrival_month_December', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('arrival_month_February', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('arrival_month_January', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('arrival_month_July', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('arrival_month_June', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('arrival_month_March', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('arrival_month_May', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('arrival_month_November', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('arrival_month_October', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('arrival_month_September', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('meal_BB', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('meal_FB', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('meal_HB', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('meal_SC', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_A', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_B', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_C', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_D', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_E', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_F', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('reserved_room_type_G', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('adr', 'FLOAT64', mode='NULLABLE'),
            bigquery.SchemaField('predicted', 'FLOAT64', mode='NULLABLE')
        ]



        # Create Table Object
        table_eda = bigquery.Table(table_id_h_eda, schema=eda_schema)
        table_ml_cancel = bigquery.Table(table_id_h_ml_cancel, schema=ml_cancel_schema)
        table_ml_price = bigquery.Table(table_id_h_ml_price, schema=ml_price_schema)

        # Create Table in BigQuery
        table_eda_created = client.create_table(table_eda)
        table_ml_cancel_created = client.create_table(table_ml_cancel)
        table_ml_price_created = client.create_table(table_ml_price)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, 
            skip_leading_rows=1, 
            autodetect= True, 
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        # Load Batch Data to BigQuery
        job1 = client.load_table_from_file(hotel_booking_eda_csv_stream, table_id_h_eda, job_config=job_config)
        job2 = client.load_table_from_file(hotel_booking_ml_cancel_csv_stream, table_id_h_ml_cancel, job_config=job_config)
        job3 = client.load_table_from_file(hotel_booking_ml_price_csv_stream, table_id_h_ml_price, job_config=job_config)

        job1.result()
        job2.result()
        job3.result()


    def load_airbnb(**kwargs):
        ti = kwargs['ti']
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{ddir}/BigQuery/is3107-g2-381308-b948b933d07a.json'

        # Create a client object
        client = bigquery.Client()
        
        airbnb_eda_csv = ti.xcom_pull(task_ids = 'transform_airbnb', key = 'airbnb_eda')
        airbnb_eda_csv_bytes = bytes(airbnb_eda_csv, 'utf-8')
        airbnb_eda_csv_stream = io.BytesIO(airbnb_eda_csv_bytes)

        # Set Table ID
        table_id_a_eda = 'is3107-g2-381308.airbnb.airbnb_eda'

        # Define Table Schema
        eda_schema = [
            bigquery.SchemaField('listing_id', 'INTEGER', mode='REQUIRED'),
            bigquery.SchemaField('host_id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('room_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('neighbourhood', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('reviews', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('overall_satisfaction', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('accommodates', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('bedrooms', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('price', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('minstay', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('latitude', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('longitude', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('date', 'DATE', mode='NULLABLE')
        ]

        # Create Table Object
        table_eda = bigquery.Table(table_id_a_eda, schema=eda_schema)

        # Create Table in BigQuery
        table = client.create_table(table_eda)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, 
            skip_leading_rows=1, 
            autodetect= True, 
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        job = client.load_table_from_file(airbnb_eda_csv_stream, table_id_a_eda, job_config=job_config)
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


extract >> [ transform_hotel, transform_airbnb ] 
transform_hotel >> load_hotel
transform_airbnb >> load_airbnb
