from google.cloud import bigquery
import os
import io
import csv
import pickle
import numpy as np
import pandas as pd

# Set the path to your service account key file
ddir = '/Users/stonley/Desktop/IS3107_G2'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{ddir}/BigQuery/is3107-g2-381308-b948b933d07a.json'

#### Run Job By Job To Test (Else rename job with job+number)

### For Batch ETL DAG

## Testing Hotel Booking Dataset For EDA
hotel_booking_dir = f'{ddir}/Dataset/batch_data/city_hotel_bookings_updated.csv'
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

hotel_booking_eda_csv = hotel_booking_eda.to_csv()
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
airbnb_dir = f'{ddir}/Dataset/batch_data/data_airbnb_raw.csv'
airbnb_file = open(airbnb_dir)
airbnb_df = pd.read_csv(airbnb_file)
csv_reader = csv.reader(airbnb_df, delimiter=',')

eda_included_cols = ['host_id', 'room_type', 'neighbourhood', 'reviews',
                     'overall_satisfaction', 'accommodates', 'bedrooms', 'price', 'minstay',
                     'latitude', 'longitude', 'date']

airbnb_eda = airbnb_df[eda_included_cols]

airbnb_eda_csv = airbnb_eda.to_csv()
airbnb_eda_csv_bytes = bytes(airbnb_eda_csv, 'utf-8')
airbnb_eda_csv_stream = io.BytesIO(airbnb_eda_csv_bytes)

with open(airbnb_dir, 'rb') as file:
    print(type(file))

# Create a client object
client = bigquery.Client()

table_id_a_eda = 'is3107-g2-381308.airbnb.airbnb_eda'

eda_schema = [
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
hotel_booking_dir = f'{ddir}/Dataset/batch_data/city_hotel_bookings_updated.csv'
hotel_booking_file = open(hotel_booking_dir)
hotel_booking_df = pd.read_csv(hotel_booking_file)
csv_reader = csv.reader(hotel_booking_df, delimiter=',')

# For ML Cancel 
ml_cancel_included_cols = ['Booking_ID', 'adults', 'children', 'stays_in_weekend_nights',
                           'stays_in_week_nights', 'meal', 'required_car_parking_spaces',
                           'reserved_room_type', 'lead_time', 'arrival_date_year',
                           'arrival_date_month', 'arrival_date_day_of_month', 'market_segment',
                           'is_repeated_guest', 'previous_cancellations',
                           'previous_bookings_not_canceled', 'adr', 'total_of_special_requests',
                           'is_canceled']

hotel_booking_ml_cancel = hotel_booking_df[ml_cancel_included_cols]
hotel_booking_ml_cancel['predicted'] = pd.Series([0] * len(hotel_booking_ml_cancel))

hotel_booking_ml_cancel_csv = hotel_booking_ml_cancel.to_csv()
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
    bigquery.SchemaField('predicted', 'INT64', mode='NULLABLE'),
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
                          'arrival_date_month', 'arrival_date_day_of_month', 
                          'market_segment', 'is_repeated_guest', 'previous_cancellations',
                          'previous_bookings_not_canceled', 'adr', 'total_of_special_requests']

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

hotel_booking_ml_price_csv = hotel_booking_ml_price.to_csv()
hotel_booking_ml_price_csv_bytes = bytes(hotel_booking_ml_price_csv, 'utf-8')
hotel_booking_ml_price_csv_stream = io.BytesIO(hotel_booking_ml_price_csv_bytes)

with open(hotel_booking_dir, 'rb') as file:
    print(type(file))

# Create a client object
client = bigquery.Client()

table_id_h_ml_price = 'is3107-g2-381308.hotel_booking.hotel_booking_ml_price'

ml_price_schema = [
    bigquery.SchemaField('Booking_ID', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('adults', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('children', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('stays_in_weekend_nights', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('stays_in_week_nights', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('meal_BB', 'STRING', mode='NULLABLE'),
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
    bigquery.SchemaField('arrival_month_January', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('arrival_month_February', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('arrival_month_March', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('arrival_month_April', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('arrival_month_May', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('arrival_month_June', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('arrival_month_July', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('arrival_month_August', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('arrival_month_September', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('arrival_month_October', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('arrival_month_December', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('arrival_date_day_of_month', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('market_segment_Complementary', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('market_segment_Corporate', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('market_segment_Direct', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('market_segment_Groups', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('market_segment_Offline_TA_TO', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('market_segment_Online_TA', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('is_repeated_guest', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('previous_cancellations', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('previous_bookings_not_canceled', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('total_of_special_requests', 'INT64', mode='NULLABLE'),
    bigquery.SchemaField('adr', 'FLOAT64', mode='NULLABLE'),
    bigquery.SchemaField('predicted', 'INT64', mode='NULLABLE'),
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


### For Streaming ETL DAG

## For Model Price
price_model_dir = ddir + '/Dashboard/Models/price_model.pkl'

## Testing Hotel Reservation Dataset For Modelling
hotel_streaming_dir = f'{ddir}/Dataset/hotel_streaming/2017-7.csv'
hotel_streaming_file = open(hotel_streaming_dir)
hotel_streaming_df = pd.read_csv(hotel_streaming_file)
csv_reader = csv.reader(hotel_streaming_df, delimiter=',')

## Data Preprocessing for Price Prediction  
processed_booking_price_df = hotel_streaming_df.copy()

# Filling Missing Data for Children
processed_booking_price_df.children.fillna(0, inplace = True)
processed_booking_price_df['children'] = processed_booking_price_df['children'].astype('int') 

# Cleaning Data for adr
processed_booking_price_df.drop(index = [row for row in processed_booking_price_df.index 
                                        if 400 < processed_booking_price_df.loc[row, 'adr']], 
                                inplace = True)
processed_booking_price_df.drop(index = [row for row in processed_booking_price_df.index 
                                        if 0 >= processed_booking_price_df.loc[row, 'adr']], 
                                inplace = True)
processed_booking_price_df.adr = processed_booking_price_df.adr.round()
processed_booking_price_df.adr = processed_booking_price_df.adr.astype('int')

## One-Hot Encoding
# For market_segment Column
market_segment_one_hot = pd.get_dummies(processed_booking_price_df['market_segment'], prefix='market_segment')
processed_booking_price_df = pd.concat([processed_booking_price_df, market_segment_one_hot], axis=1)
processed_booking_price_df.drop('market_segment', axis=1, inplace=True)

# For arrival_date_month Column
arrival_date_month_one_hot = pd.get_dummies(processed_booking_price_df['arrival_date_month'], prefix='arrival_date_month')
processed_booking_price_df = pd.concat([processed_booking_price_df, arrival_date_month_one_hot], axis=1)
processed_booking_price_df.drop('arrival_date_month', axis=1, inplace=True)

# For meal Column
meal_one_hot = pd.get_dummies(processed_booking_price_df['meal'], prefix='meal')
processed_booking_price_df = pd.concat([processed_booking_price_df, meal_one_hot], axis=1)
processed_booking_price_df.drop('meal', axis=1, inplace=True)

# For reserved_room_type Column
reserved_room_type_one_hot = pd.get_dummies(processed_booking_price_df['reserved_room_type'], prefix='reserved_room_type')
processed_booking_price_df = pd.concat([processed_booking_price_df, reserved_room_type_one_hot], axis=1)
processed_booking_price_df.drop('reserved_room_type', axis=1, inplace=True)

## Load Pretrained price_model using pickle
with open(price_model_dir, 'wb') as f:
    price_model = pickle.load(f)

# variables
predictors = processed_booking_price_df.iloc[:,:-1]

# Predict the price for the customer
predicted_price = price_model.predict(predictors)

# Append the predicted price back to original df
processed_booking_price_df['predicted'] = predicted_price

hotel_streaming_ml_price_csv = processed_booking_price_df.to_csv()
hotel_streaming_ml_price_csv_bytes = bytes(hotel_streaming_ml_price_csv, 'utf-8')
hotel_streaming_ml_price_csv_stream = io.BytesIO(hotel_streaming_ml_price_csv_bytes)

# Table ID
table_id = 'is3107-g2-381308.hotel_booking.hotel_booking_ml_price'

# Table Ref
table = client.get_table(table_id)

job_config = bigquery.LoadJobConfig(
    skip_leading_rows = 1,
    source_format = bigquery.SourceFormat().CSV,
    # Append    
    write_disposition = bigquery.WriteDisposition.WRITE_APPEND
)

# Append the streaming data to the table
job = client.load_table_from_file(hotel_streaming_ml_price_csv_stream, table_id, job_config=job_config)

job.result()