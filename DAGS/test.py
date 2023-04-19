from google.cloud import bigquery
import os
import io
import csv
import numpy as np
import pandas as pd


# Set the path to your service account key file
ddir = 'C:\\Users\\KMwong\\Desktop\\IS3107\\Projects\\IS3107_G2'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{ddir}\\BigQuery\\is3107-g2-381308-b948b933d07a.json'

#### Run Job By Job To Test (Else rename job with job+number)

### For Batch ETL DAG

## Testing Hotel Booking Dataset For EDA
hotel_booking_dir = f'{ddir}\\Dataset\\batch_data\\city_hotel_bookings_updated.csv'
hotel_booking_file = open(hotel_booking_dir)
hotel_booking_df = pd.read_csv(hotel_booking_file)
csv_reader = csv.reader(hotel_booking_df, delimiter=',')

eda_included_cols = ['Booking_ID', 'adults', 'children', 'stays_in_weekend_nights',
                     'stays_in_week_nights', 'meal', 'required_car_parking_spaces',
                     'reserved_room_type', 'lead_time', 'arrival_date_month',
                     'arrival_date_day_of_month', 'market_segment', 'is_repeated_guest',
                     'previous_cancellations', 'previous_bookings_not_canceled', 'adr', 
                     'total_of_special_requests', 'is_canceled']

hotel_booking_eda = hotel_booking_df[eda_included_cols]

hotel_booking_eda_csv = hotel_booking_eda.to_csv(index=False)
hotel_booking_eda_csv_bytes = bytes(hotel_booking_eda_csv, 'utf-8')
hotel_booking_eda_csv_stream = io.BytesIO(hotel_booking_eda_csv_bytes)

with open(hotel_booking_dir, 'rb') as file:
    print(type(file))

# Create a client object
client = bigquery.Client()

table_id_h_eda = 'is3107-g2-381308.hotel_booking.hotel_booking_eda'

eda_schema = [
    bigquery.SchemaField('Booking_ID', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('adults', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('children', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('stays_in_weekend_nights', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('stays_in_week_nights', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('meal', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('required_car_parking_spaces', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('reserved_room_type ', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('lead_time', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('arrival_date_year', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('arrival_date_month', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('arrival_date_day_of_month', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('market_segment', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('is_repeated_guest', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('previous_cancellations', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('previous_bookings_not_canceled', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('adr', 'FLOAT64', mode='NULLABLE'),
    bigquery.SchemaField('total_of_special_requests', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('is_canceled', 'INT64', mode='NULLABLE'),
]

table_eda = bigquery.Table(table_id_h_eda, schema=eda_schema)

table = client.create_table(table_eda)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV, 
    skip_leading_rows=1, autodetect= True,
     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
)

job = client.load_table_from_file(hotel_booking_eda_csv_stream, table_id_h_eda, job_config=job_config)

job.result()


## Testing Airbnb Dataset For EDA
airbnb_dir = f'{ddir}\\Dataset\\batch_data\\data_airbnb_raw.csv'
airbnb_file = open(airbnb_dir)
airbnb_df = pd.read_csv(airbnb_file)
csv_reader = csv.reader(airbnb_df, delimiter=',')

eda_included_cols = ['listing_id', 'host_id', 'room_type', 'neighbourhood', 'reviews',
                     'overall_satisfaction', 'accommodates', 'bedrooms', 'price', 'minstay',
                     'latitude', 'longitude', 'date']

airbnb_eda = airbnb_df[eda_included_cols]

airbnb_eda_csv = airbnb_eda.to_csv(index=False)
airbnb_eda_csv_bytes = bytes(airbnb_eda_csv, 'utf-8')
airbnb_eda_csv_stream = io.BytesIO(airbnb_eda_csv_bytes)

with open(airbnb_dir, 'rb') as file:
    print(type(file))

# Create a client object
client = bigquery.Client()

table_id_a_eda = 'is3107-g2-381308.airbnb.airbnb_eda'

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

table_eda = bigquery.Table(table_id_a_eda, schema=eda_schema)

table_eda_created = client.create_table(table_eda)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1, autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
)

job = client.load_table_from_file(
    airbnb_eda_csv_stream, table_id_a_eda, job_config=job_config)

job.result()


## Testing Hotel Booking Dataset For Modelling
hotel_booking_dir = f'{ddir}\\Dataset\\batch_data\\city_hotel_bookings_updated.csv'
hotel_booking_file = open(hotel_booking_dir)
hotel_booking_df = pd.read_csv(hotel_booking_file)
csv_reader = csv.reader(hotel_booking_df, delimiter=',')

# For ML Cancel 
ml_cancel_included_cols = ['Booking_ID', 'adults', 'children', 'stays_in_weekend_nights',
                           'stays_in_week_nights', 'meal', 'required_car_parking_spaces',
                           'reserved_room_type', 'lead_time', 'arrival_date_month', 
                           'market_segment', 'is_repeated_guest', 'previous_cancellations',
                           'previous_bookings_not_canceled', 'adr', 'total_of_special_requests',
                           'is_canceled']

hotel_booking_ml_cancel = hotel_booking_df[ml_cancel_included_cols]

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

# Variables 
cols = ['arrival_date_month_April', 'arrival_date_month_August', 'arrival_date_month_December', 
        'arrival_date_month_February', 'arrival_date_month_January', 'arrival_date_month_July',
        'arrival_date_month_June', 'arrival_date_month_March', 'arrival_date_month_May',
        'arrival_date_month_November', 'arrival_date_month_October', 'arrival_date_month_September',
        'meal_BB', 'meal_FB', 'meal_HB', 'meal_SC', 'reserved_room_type_A', 'reserved_room_type_B',
        'reserved_room_type_C', 'reserved_room_type_D', 'reserved_room_type_E', 'reserved_room_type_F',
        'reserved_room_type_G',  'market_segment_Aviation', 'market_segment_Complementary', 
        'market_segment_Corporate', 'market_segment_Direct', 'market_segment_Groups', 
        'market_segment_Offline TA/TO', 'market_segment_Online TA', 'is_repeated_guest', 'lead_time', 
        'arrival_date_year', 'stays_in_weekend_nights', 'stays_in_week_nights', 'adults', 'children', 
        'previous_cancellations', 'previous_bookings_not_canceled', 'adr','required_car_parking_spaces', 
        'total_of_special_requests', 'is_canceled'
        ]

processed_booking_ml_cancel_df = pd.DataFrame(columns=cols)
processed_booking_ml_cancel_df = pd.concat([processed_booking_ml_cancel_df, hotel_booking_ml_cancel])
processed_booking_ml_cancel_df.fillna(0, inplace=True)

processed_booking_ml_cancel_df['predicted'] = pd.Series([0] * len(processed_booking_ml_cancel_df))

processed_booking_ml_cancel_df['market_segment_Aviation'] = processed_booking_ml_cancel_df['market_segment_Aviation'].astype('int') 
processed_booking_ml_cancel_df['market_segment_Complementary'] = processed_booking_ml_cancel_df['market_segment_Complementary'].astype('int') 
processed_booking_ml_cancel_df['market_segment_Corporate'] = processed_booking_ml_cancel_df['market_segment_Corporate'].astype('int')
processed_booking_ml_cancel_df['market_segment_Direct'] = processed_booking_ml_cancel_df['market_segment_Direct'].astype('int')
processed_booking_ml_cancel_df['market_segment_Groups'] = processed_booking_ml_cancel_df['market_segment_Groups'].astype('int')
processed_booking_ml_cancel_df['market_segment_Offline TA/TO'] = processed_booking_ml_cancel_df['market_segment_Offline TA/TO'].astype('int')
processed_booking_ml_cancel_df['market_segment_Online TA'] = processed_booking_ml_cancel_df['market_segment_Online TA'].astype('int')
processed_booking_ml_cancel_df['arrival_date_month_April'] = processed_booking_ml_cancel_df['arrival_date_month_April'].astype('int')
processed_booking_ml_cancel_df['arrival_date_month_August'] = processed_booking_ml_cancel_df['arrival_date_month_August'].astype('int')
processed_booking_ml_cancel_df['arrival_date_month_December'] = processed_booking_ml_cancel_df['arrival_date_month_December'].astype('int')
processed_booking_ml_cancel_df['arrival_date_month_February'] = processed_booking_ml_cancel_df['arrival_date_month_February'].astype('int')
processed_booking_ml_cancel_df['arrival_date_month_January'] = processed_booking_ml_cancel_df['arrival_date_month_January'].astype('int')
processed_booking_ml_cancel_df['arrival_date_month_July'] = processed_booking_ml_cancel_df['arrival_date_month_July'].astype('int')
processed_booking_ml_cancel_df['arrival_date_month_June'] = processed_booking_ml_cancel_df['arrival_date_month_June'].astype('int')
processed_booking_ml_cancel_df['arrival_date_month_March'] = processed_booking_ml_cancel_df['arrival_date_month_March'].astype('int')
processed_booking_ml_cancel_df['arrival_date_month_May'] = processed_booking_ml_cancel_df['arrival_date_month_May'].astype('int')
processed_booking_ml_cancel_df['arrival_date_month_November'] = processed_booking_ml_cancel_df['arrival_date_month_November'].astype('int')
processed_booking_ml_cancel_df['arrival_date_month_October'] = processed_booking_ml_cancel_df['arrival_date_month_October'].astype('int')
processed_booking_ml_cancel_df['arrival_date_month_September'] = processed_booking_ml_cancel_df['arrival_date_month_September'].astype('int')
processed_booking_ml_cancel_df['meal_BB'] = processed_booking_ml_cancel_df['meal_BB'].astype('int')
processed_booking_ml_cancel_df['meal_FB'] = processed_booking_ml_cancel_df['meal_FB'].astype('int')
processed_booking_ml_cancel_df['meal_HB'] = processed_booking_ml_cancel_df['meal_HB'].astype('int')
processed_booking_ml_cancel_df['meal_SC'] = processed_booking_ml_cancel_df['meal_SC'].astype('int')
processed_booking_ml_cancel_df['reserved_room_type_A'] = processed_booking_ml_cancel_df['reserved_room_type_A'].astype('int')
processed_booking_ml_cancel_df['reserved_room_type_B'] = processed_booking_ml_cancel_df['reserved_room_type_B'].astype('int')
processed_booking_ml_cancel_df['reserved_room_type_C'] = processed_booking_ml_cancel_df['reserved_room_type_C'].astype('int')
processed_booking_ml_cancel_df['reserved_room_type_D'] = processed_booking_ml_cancel_df['reserved_room_type_D'].astype('int')
processed_booking_ml_cancel_df['reserved_room_type_E'] = processed_booking_ml_cancel_df['reserved_room_type_E'].astype('int')
processed_booking_ml_cancel_df['reserved_room_type_F'] = processed_booking_ml_cancel_df['reserved_room_type_F'].astype('int')
processed_booking_ml_cancel_df['reserved_room_type_G'] = processed_booking_ml_cancel_df['reserved_room_type_G'].astype('int')

hotel_booking_ml_cancel_csv = processed_booking_ml_cancel_df.to_csv(index=False)
hotel_booking_ml_cancel_csv_bytes = bytes(hotel_booking_ml_cancel_csv, 'utf-8')
hotel_booking_ml_cancel_csv_stream = io.BytesIO(hotel_booking_ml_cancel_csv_bytes)

with open(hotel_booking_dir, 'rb') as file:
    print(type(file))

# Create a client object
client = bigquery.Client()

table_id_h_ml_cancel = 'is3107-g2-381308.hotel_booking.hotel_booking_ml_cancel'

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
    bigquery.SchemaField('lead_time', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('arrival_date_year', 'INT64', mode='NULLABLE'),
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
    bigquery.SchemaField('market_segment_Offline_TA_TO', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('market_segment_Online_TA', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('is_repeated_guest', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('previous_cancellations', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('previous_bookings_not_canceled', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('adr', 'FLOAT64', mode='NULLABLE'),
    bigquery.SchemaField('total_of_special_requests', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('is_canceled', 'INT64', mode='NULLABLE')
]

table_ml_cancel = bigquery.Table(table_id_h_ml_cancel, schema=ml_cancel_schema)
table_ml_cancel_created = client.create_table(table_ml_cancel)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1, autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
)

job = client.load_table_from_file(
    hotel_booking_ml_cancel_csv_stream, table_id_h_ml_cancel, job_config=job_config)

job.result()

# For ML Price
ml_price_included_cols = ['Booking_ID','adults', 'children', 'stays_in_weekend_nights',
                          'stays_in_week_nights', 'meal', 'required_car_parking_spaces',
                          'reserved_room_type', 'lead_time', 'arrival_date_year',
                          'arrival_date_month', 'market_segment', 'is_repeated_guest', 
                          'previous_cancellations', 'previous_bookings_not_canceled', 
                          'adr', 'total_of_special_requests']

hotel_booking_ml_price = hotel_booking_df[ml_price_included_cols]

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
                                    if 50 >= hotel_booking_ml_price.loc[row, 'adr']], 
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

# Variables 
cols = ['arrival_date_month_April', 'arrival_date_month_August', 'arrival_date_month_December', 
        'arrival_date_month_February', 'arrival_date_month_January', 'arrival_date_month_July',
        'arrival_date_month_June', 'arrival_date_month_March', 'arrival_date_month_May',
        'arrival_date_month_November', 'arrival_date_month_October', 'arrival_date_month_September',
        'meal_BB', 'meal_FB', 'meal_HB', 'meal_SC', 'reserved_room_type_A', 'reserved_room_type_B',
        'reserved_room_type_C', 'reserved_room_type_D', 'reserved_room_type_E', 'reserved_room_type_F',
        'reserved_room_type_G',  
        'market_segment_Aviation', 'market_segment_Complementary', 'market_segment_Corporate', 'market_segment_Direct',
        'market_segment_Groups', 'market_segment_Offline TA/TO', 'market_segment_Online TA',
        'is_repeated_guest', 'lead_time', 'stays_in_weekend_nights', 'stays_in_week_nights',
        'adults', 'children', 'previous_cancellations', 'previous_bookings_not_canceled', 'adr','required_car_parking_spaces', 'total_of_special_requests', 'arrival_date_year'
       ]

processed_booking_ml_price_df = pd.DataFrame(columns=cols)
processed_booking_ml_price_df = pd.concat([processed_booking_ml_price_df, hotel_booking_ml_price])
processed_booking_ml_price_df.fillna(0, inplace=True)
processed_booking_ml_price_df['predicted'] = pd.Series([0] * len(processed_booking_ml_price_df))

processed_booking_ml_price_df['market_segment_Aviation'] = processed_booking_ml_price_df['market_segment_Aviation'].astype('int') 
processed_booking_ml_price_df['market_segment_Complementary'] = processed_booking_ml_price_df['market_segment_Complementary'].astype('int') 
processed_booking_ml_price_df['market_segment_Corporate'] = processed_booking_ml_price_df['market_segment_Corporate'].astype('int')
processed_booking_ml_price_df['market_segment_Direct'] = processed_booking_ml_price_df['market_segment_Direct'].astype('int')
processed_booking_ml_price_df['market_segment_Groups'] = processed_booking_ml_price_df['market_segment_Groups'].astype('int')
processed_booking_ml_price_df['market_segment_Offline TA/TO'] = processed_booking_ml_price_df['market_segment_Offline TA/TO'].astype('int')
processed_booking_ml_price_df['market_segment_Online TA'] = processed_booking_ml_price_df['market_segment_Online TA'].astype('int')
processed_booking_ml_price_df['arrival_date_month_April'] = processed_booking_ml_price_df['arrival_date_month_April'].astype('int')
processed_booking_ml_price_df['arrival_date_month_August'] = processed_booking_ml_price_df['arrival_date_month_August'].astype('int')
processed_booking_ml_price_df['arrival_date_month_December'] = processed_booking_ml_price_df['arrival_date_month_December'].astype('int')
processed_booking_ml_price_df['arrival_date_month_February'] = processed_booking_ml_price_df['arrival_date_month_February'].astype('int')
processed_booking_ml_price_df['arrival_date_month_January'] = processed_booking_ml_price_df['arrival_date_month_January'].astype('int')
processed_booking_ml_price_df['arrival_date_month_July'] = processed_booking_ml_price_df['arrival_date_month_July'].astype('int')
processed_booking_ml_price_df['arrival_date_month_June'] = processed_booking_ml_price_df['arrival_date_month_June'].astype('int')
processed_booking_ml_price_df['arrival_date_month_March'] = processed_booking_ml_price_df['arrival_date_month_March'].astype('int')
processed_booking_ml_price_df['arrival_date_month_May'] = processed_booking_ml_price_df['arrival_date_month_May'].astype('int')
processed_booking_ml_price_df['arrival_date_month_November'] = processed_booking_ml_price_df['arrival_date_month_November'].astype('int')
processed_booking_ml_price_df['arrival_date_month_October'] = processed_booking_ml_price_df['arrival_date_month_October'].astype('int')
processed_booking_ml_price_df['arrival_date_month_September'] = processed_booking_ml_price_df['arrival_date_month_September'].astype('int')
processed_booking_ml_price_df['meal_BB'] = processed_booking_ml_price_df['meal_BB'].astype('int')
processed_booking_ml_price_df['meal_FB'] = processed_booking_ml_price_df['meal_FB'].astype('int')
processed_booking_ml_price_df['meal_HB'] = processed_booking_ml_price_df['meal_HB'].astype('int')
processed_booking_ml_price_df['meal_SC'] = processed_booking_ml_price_df['meal_SC'].astype('int')
processed_booking_ml_price_df['reserved_room_type_A'] = processed_booking_ml_price_df['reserved_room_type_A'].astype('int')
processed_booking_ml_price_df['reserved_room_type_B'] = processed_booking_ml_price_df['reserved_room_type_B'].astype('int')
processed_booking_ml_price_df['reserved_room_type_C'] = processed_booking_ml_price_df['reserved_room_type_C'].astype('int')
processed_booking_ml_price_df['reserved_room_type_D'] = processed_booking_ml_price_df['reserved_room_type_D'].astype('int')
processed_booking_ml_price_df['reserved_room_type_E'] = processed_booking_ml_price_df['reserved_room_type_E'].astype('int')
processed_booking_ml_price_df['reserved_room_type_F'] = processed_booking_ml_price_df['reserved_room_type_F'].astype('int')
processed_booking_ml_price_df['reserved_room_type_G'] = processed_booking_ml_price_df['reserved_room_type_G'].astype('int')

hotel_booking_ml_price_csv = processed_booking_ml_price_df.to_csv(index=False)
hotel_booking_ml_price_csv_bytes = bytes(hotel_booking_ml_price_csv, 'utf-8')
hotel_booking_ml_price_csv_stream = io.BytesIO(hotel_booking_ml_price_csv_bytes)

with open(hotel_booking_dir, 'rb') as file:
    print(type(file))

# Create a client object
client = bigquery.Client()

table_id_h_ml_price = 'is3107-g2-381308.hotel_booking.hotel_booking_ml_price'

ml_price_schema = [
    bigquery.SchemaField('Booking_ID', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('lead_time', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('arrival_date_year', 'INT64', mode='NULLABLE'),
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

table_ml_price = bigquery.Table(table_id_h_ml_price, schema=ml_price_schema)
table_ml_price_created = client.create_table(table_ml_price)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1, autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
)

job = client.load_table_from_file(
    hotel_booking_ml_price_csv_stream, table_id_h_ml_price, job_config=job_config)

job.result()