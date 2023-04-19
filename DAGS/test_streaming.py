from google.cloud import bigquery
import os
import io
import csv
import pickle
import numpy as np
import pandas as pd

# Set the path to your service account key file
ddir = 'C:\\Users\\KMwong\\Desktop\\IS3107\\Projects\\IS3107_G2'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{ddir}\\BigQuery\\is3107-g2-381308-b948b933d07a.json'

#### Run Job By Job To Test (Else rename job with job+number)

### For Streaming ETL DAG

## Testing Hotel Reservation Dataset For Modelling
hotel_streaming_dir = f'{ddir}\\Dataset\\hotel_streaming\\2017-7.csv'
hotel_streaming_file = open(hotel_streaming_dir)
hotel_streaming_df = pd.read_csv(hotel_streaming_file)
csv_reader = csv.reader(hotel_streaming_df, delimiter=',')

## For Model Price
price_model_dir = ddir + '\\Dashboard\\Models\\price_model.pkl'

## Data Preprocessing for Price Prediction  
booking_price_df = hotel_streaming_df.copy()

# Filling Missing Data for Children
booking_price_df.children.fillna(0, inplace = True)
booking_price_df['children'] = booking_price_df['children'].astype('int') 

# Cleaning Data for adr
booking_price_df.drop(index = [row for row in booking_price_df.index 
                                if 400 < booking_price_df.loc[row, 'adr']], 
                      inplace = True)
booking_price_df.drop(index = [row for row in booking_price_df.index 
                                if 50 >= booking_price_df.loc[row, 'adr']], 
                      inplace = True)
booking_price_df.adr = booking_price_df.adr.round()
booking_price_df.adr = booking_price_df.adr.astype('int')

## One-Hot Encoding
# For market_segment Column
market_segment_one_hot = pd.get_dummies(booking_price_df['market_segment'], prefix='market_segment')
booking_price_df = pd.concat([booking_price_df, market_segment_one_hot], axis=1)
booking_price_df.drop('market_segment', axis=1, inplace=True)

# For arrival_date_month Column
arrival_date_month_one_hot = pd.get_dummies(booking_price_df['arrival_date_month'], prefix='arrival_date_month')
booking_price_df = pd.concat([booking_price_df, arrival_date_month_one_hot], axis=1)
booking_price_df.drop('arrival_date_month', axis=1, inplace=True)

# For meal Column
meal_one_hot = pd.get_dummies(booking_price_df['meal'], prefix='meal')
booking_price_df = pd.concat([booking_price_df, meal_one_hot], axis=1)
booking_price_df.drop('meal', axis=1, inplace=True)

# For reserved_room_type Column
reserved_room_type_one_hot = pd.get_dummies(booking_price_df['reserved_room_type'], prefix='reserved_room_type')
booking_price_df = pd.concat([booking_price_df, reserved_room_type_one_hot], axis=1)
booking_price_df.drop('reserved_room_type', axis=1, inplace=True)

# Variables
cols = ['lead_time', 'adr', 'arrival_date_year', 'stays_in_weekend_nights', 
        'stays_in_week_nights', 'adults', 'children', 'is_repeated_guest', 
        'previous_cancellations', 'previous_bookings_not_canceled',
        'required_car_parking_spaces', 'total_of_special_requests', 
        'market_segment_Aviation', 'market_segment_Complementary', 
        'market_segment_Corporate', 'market_segment_Direct', 
        'market_segment_Groups', 'market_segment_Offline TA/TO', 
        'market_segment_Online TA', 'arrival_date_month_April',
        'arrival_date_month_August', 'arrival_date_month_December',
        'arrival_date_month_February', 'arrival_date_month_January',
        'arrival_date_month_July', 'arrival_date_month_June',
        'arrival_date_month_March', 'arrival_date_month_May',
        'arrival_date_month_November', 'arrival_date_month_October', 
        'arrival_date_month_September', 'meal_BB', 'meal_FB', 'meal_HB', 
        'meal_SC', 'reserved_room_type_A', 'reserved_room_type_B', 
        'reserved_room_type_C', 'reserved_room_type_D', 'reserved_room_type_E', 
        'reserved_room_type_F', 'reserved_room_type_G', 'Booking_ID'
       ]

## Setting Data
processed_booking_price_df = pd.DataFrame(columns=cols)
processed_booking_price_df = pd.concat([processed_booking_price_df, booking_price_df])
processed_booking_price_df.fillna(0, inplace = True)
processed_booking_price_df.drop('is_canceled', axis=1, inplace=True)
processed_booking_price_df.drop('arrival_date_day_of_month', axis=1, inplace=True)
processed_booking_price_df['Booking_ID'] = processed_booking_price_df['Booking_ID'].str[3:]
processed_booking_price_df['Booking_ID'] = processed_booking_price_df['Booking_ID'].astype('int') 
processed_booking_price_df['market_segment_Aviation'] = processed_booking_price_df['market_segment_Aviation'].astype('int') 
processed_booking_price_df['market_segment_Complementary'] = processed_booking_price_df['market_segment_Complementary'].astype('int') 
processed_booking_price_df['market_segment_Corporate'] = processed_booking_price_df['market_segment_Corporate'].astype('int')
processed_booking_price_df['market_segment_Direct'] = processed_booking_price_df['market_segment_Direct'].astype('int')
processed_booking_price_df['market_segment_Groups'] = processed_booking_price_df['market_segment_Groups'].astype('int')
processed_booking_price_df['market_segment_Offline TA/TO'] = processed_booking_price_df['market_segment_Offline TA/TO'].astype('int')
processed_booking_price_df['market_segment_Online TA'] = processed_booking_price_df['market_segment_Online TA'].astype('int')
processed_booking_price_df['arrival_date_month_April'] = processed_booking_price_df['arrival_date_month_April'].astype('int')
processed_booking_price_df['arrival_date_month_August'] = processed_booking_price_df['arrival_date_month_August'].astype('int')
processed_booking_price_df['arrival_date_month_December'] = processed_booking_price_df['arrival_date_month_December'].astype('int')
processed_booking_price_df['arrival_date_month_February'] = processed_booking_price_df['arrival_date_month_February'].astype('int')
processed_booking_price_df['arrival_date_month_January'] = processed_booking_price_df['arrival_date_month_January'].astype('int')
processed_booking_price_df['arrival_date_month_July'] = processed_booking_price_df['arrival_date_month_July'].astype('int')
processed_booking_price_df['arrival_date_month_June'] = processed_booking_price_df['arrival_date_month_June'].astype('int')
processed_booking_price_df['arrival_date_month_March'] = processed_booking_price_df['arrival_date_month_March'].astype('int')
processed_booking_price_df['arrival_date_month_May'] = processed_booking_price_df['arrival_date_month_May'].astype('int')
processed_booking_price_df['arrival_date_month_November'] = processed_booking_price_df['arrival_date_month_November'].astype('int')
processed_booking_price_df['arrival_date_month_October'] = processed_booking_price_df['arrival_date_month_October'].astype('int')
processed_booking_price_df['arrival_date_month_September'] = processed_booking_price_df['arrival_date_month_September'].astype('int')
processed_booking_price_df['meal_BB'] = processed_booking_price_df['meal_BB'].astype('int')
processed_booking_price_df['meal_FB'] = processed_booking_price_df['meal_FB'].astype('int')
processed_booking_price_df['meal_HB'] = processed_booking_price_df['meal_HB'].astype('int')
processed_booking_price_df['meal_SC'] = processed_booking_price_df['meal_SC'].astype('int')
processed_booking_price_df['reserved_room_type_A'] = processed_booking_price_df['reserved_room_type_A'].astype('int')
processed_booking_price_df['reserved_room_type_B'] = processed_booking_price_df['reserved_room_type_B'].astype('int')
processed_booking_price_df['reserved_room_type_C'] = processed_booking_price_df['reserved_room_type_C'].astype('int')
processed_booking_price_df['reserved_room_type_D'] = processed_booking_price_df['reserved_room_type_D'].astype('int')
processed_booking_price_df['reserved_room_type_E'] = processed_booking_price_df['reserved_room_type_E'].astype('int')
processed_booking_price_df['reserved_room_type_F'] = processed_booking_price_df['reserved_room_type_F'].astype('int')
processed_booking_price_df['reserved_room_type_G'] = processed_booking_price_df['reserved_room_type_G'].astype('int')


## Load Pretrained price_model using pickle
with open(price_model_dir, 'rb') as f:
    price_model = pickle.load(f)

# Predict the price for the customer
predictors_cols = ['lead_time', 'arrival_date_year', 'stays_in_weekend_nights', 
                   'stays_in_week_nights', 'adults', 'children', 'is_repeated_guest', 
                   'previous_cancellations', 'previous_bookings_not_canceled', 
                   'required_car_parking_spaces', 'total_of_special_requests', 
                   'market_segment_Aviation', 'market_segment_Complementary', 
                   'market_segment_Corporate', 'market_segment_Direct', 
                   'market_segment_Groups', 'market_segment_Offline TA/TO', 
                   'market_segment_Online TA', 'arrival_date_month_April',
                   'arrival_date_month_August', 'arrival_date_month_December',
                   'arrival_date_month_February', 'arrival_date_month_January',
                   'arrival_date_month_July', 'arrival_date_month_June',
                   'arrival_date_month_March', 'arrival_date_month_May',
                   'arrival_date_month_November', 'arrival_date_month_October', 
                   'arrival_date_month_September', 'meal_BB', 'meal_FB', 'meal_HB', 
                   'meal_SC', 'reserved_room_type_A', 'reserved_room_type_B', 
                   'reserved_room_type_C', 'reserved_room_type_D', 'reserved_room_type_E', 
                   'reserved_room_type_F', 'reserved_room_type_G'
                  ]

predictors = processed_booking_price_df[predictors_cols]
predicted_price = price_model.predict(predictors)

# Append the predicted price back to original df
processed_booking_price_df['predicted'] = predicted_price
processed_booking_price_df.rename(columns={'market_segment_Offline TA/TO': 'market_segment_Offline_TA_TO', 
                                           'market_segment_Online TA': 'market_segment_Online_TA'}, 
                                  inplace=True)

hotel_streaming_ml_price_csv = processed_booking_price_df.to_csv(index=False)
hotel_streaming_ml_price_csv_bytes = bytes(hotel_streaming_ml_price_csv, 'utf-8')
hotel_streaming_ml_price_csv_stream = io.BytesIO(hotel_streaming_ml_price_csv_bytes)

# Create a client object
client = bigquery.Client()

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


## For Model Cancellation
cancel_model_dir = ddir + '\\Dashboard\\Models\\cancellation_model.pkl'

## Data Preprocessing for Cancellation Prediction  
preprocessed_booking_cancel_df = hotel_streaming_df.copy()

## Pre-Processing

# Filling Missing Data for Children
preprocessed_booking_cancel_df.children.fillna(0, inplace = True)
preprocessed_booking_cancel_df['children'] = preprocessed_booking_cancel_df['children'].astype('int') 

# Cleaning Data for adr
preprocessed_booking_cancel_df.drop(index = [row for row in preprocessed_booking_cancel_df.index 
                                        if 400 < preprocessed_booking_cancel_df.loc[row, 'adr']], 
                                inplace = True)
preprocessed_booking_cancel_df.drop(index = [row for row in preprocessed_booking_cancel_df.index 
                                        if 50 >= preprocessed_booking_cancel_df.loc[row, 'adr']], 
                                inplace = True)
preprocessed_booking_cancel_df.adr = preprocessed_booking_cancel_df.adr.round()
preprocessed_booking_cancel_df.adr = preprocessed_booking_cancel_df.adr.astype('int')

## One-Hot Encoding
# For market_segment Column
market_segment_one_hot = pd.get_dummies(preprocessed_booking_cancel_df['market_segment'], prefix='market_segment')
preprocessed_booking_cancel_df = pd.concat([preprocessed_booking_cancel_df, market_segment_one_hot], axis=1)
preprocessed_booking_cancel_df.drop('market_segment', axis=1, inplace=True)

# For arrival_date_month Column
arrival_date_month_one_hot = pd.get_dummies(preprocessed_booking_cancel_df['arrival_date_month'], prefix='arrival_date_month')
preprocessed_booking_cancel_df = pd.concat([preprocessed_booking_cancel_df, arrival_date_month_one_hot], axis=1)
preprocessed_booking_cancel_df.drop('arrival_date_month', axis=1, inplace=True)

# For meal Column
meal_one_hot = pd.get_dummies(preprocessed_booking_cancel_df['meal'], prefix='meal')
preprocessed_booking_cancel_df = pd.concat([preprocessed_booking_cancel_df, meal_one_hot], axis=1)
preprocessed_booking_cancel_df.drop('meal', axis=1, inplace=True)

# For reserved_room_type Column
reserved_room_type_one_hot = pd.get_dummies(preprocessed_booking_cancel_df['reserved_room_type'], prefix='reserved_room_type')
preprocessed_booking_cancel_df = pd.concat([preprocessed_booking_cancel_df, reserved_room_type_one_hot], axis=1)
preprocessed_booking_cancel_df.drop('reserved_room_type', axis=1, inplace=True)

# Variables
cols = ['lead_time', 'adr', 'arrival_date_year',
    'stays_in_weekend_nights', 'stays_in_week_nights', 'adults', 
    'children', 'is_repeated_guest', 'previous_cancellations', 
    'previous_bookings_not_canceled', 'required_car_parking_spaces', 
    'total_of_special_requests', 'market_segment_Aviation', 
    'market_segment_Complementary', 'market_segment_Corporate', 
    'market_segment_Direct', 'market_segment_Groups', 
    'market_segment_Offline TA/TO', 'market_segment_Online TA', 
    'arrival_date_month_April', 'arrival_date_month_August', 
    'arrival_date_month_December', 'arrival_date_month_February', 
    'arrival_date_month_January', 'arrival_date_month_July', 
    'arrival_date_month_June', 'arrival_date_month_March', 
    'arrival_date_month_May', 'arrival_date_month_November', 
    'arrival_date_month_October', 'arrival_date_month_September', 
    'meal_BB', 'meal_FB', 'meal_HB', 'meal_SC', 'reserved_room_type_A', 
    'reserved_room_type_B', 'reserved_room_type_C', 'reserved_room_type_D', 
    'reserved_room_type_E', 'reserved_room_type_F', 'reserved_room_type_G', 
    'Booking_ID'
    ]

## Setting Data
processed_booking_cancel_df = pd.DataFrame(columns=cols)
processed_booking_cancel_df = pd.concat([processed_booking_cancel_df, preprocessed_booking_cancel_df])
processed_booking_cancel_df.fillna(0, inplace = True)
processed_booking_cancel_df.drop('arrival_date_day_of_month', axis=1, inplace=True)
processed_booking_cancel_df['Booking_ID'] = processed_booking_cancel_df['Booking_ID'].str[3:]
processed_booking_cancel_df['Booking_ID'] = processed_booking_cancel_df['Booking_ID'].astype('int') 
processed_booking_cancel_df['market_segment_Aviation'] = processed_booking_cancel_df['market_segment_Aviation'].astype('int') 
processed_booking_cancel_df['market_segment_Complementary'] = processed_booking_cancel_df['market_segment_Complementary'].astype('int') 
processed_booking_cancel_df['market_segment_Corporate'] = processed_booking_cancel_df['market_segment_Corporate'].astype('int')
processed_booking_cancel_df['market_segment_Direct'] = processed_booking_cancel_df['market_segment_Direct'].astype('int')
processed_booking_cancel_df['market_segment_Groups'] = processed_booking_cancel_df['market_segment_Groups'].astype('int')
processed_booking_cancel_df['market_segment_Offline TA/TO'] = processed_booking_cancel_df['market_segment_Offline TA/TO'].astype('int')
processed_booking_cancel_df['market_segment_Online TA'] = processed_booking_cancel_df['market_segment_Online TA'].astype('int')
processed_booking_cancel_df['arrival_date_month_April'] = processed_booking_cancel_df['arrival_date_month_April'].astype('int')
processed_booking_cancel_df['arrival_date_month_August'] = processed_booking_cancel_df['arrival_date_month_August'].astype('int')
processed_booking_cancel_df['arrival_date_month_December'] = processed_booking_cancel_df['arrival_date_month_December'].astype('int')
processed_booking_cancel_df['arrival_date_month_February'] = processed_booking_cancel_df['arrival_date_month_February'].astype('int')
processed_booking_cancel_df['arrival_date_month_January'] = processed_booking_cancel_df['arrival_date_month_January'].astype('int')
processed_booking_cancel_df['arrival_date_month_July'] = processed_booking_cancel_df['arrival_date_month_July'].astype('int')
processed_booking_cancel_df['arrival_date_month_June'] = processed_booking_cancel_df['arrival_date_month_June'].astype('int')
processed_booking_cancel_df['arrival_date_month_March'] = processed_booking_cancel_df['arrival_date_month_March'].astype('int')
processed_booking_cancel_df['arrival_date_month_May'] = processed_booking_cancel_df['arrival_date_month_May'].astype('int')
processed_booking_cancel_df['arrival_date_month_November'] = processed_booking_cancel_df['arrival_date_month_November'].astype('int')
processed_booking_cancel_df['arrival_date_month_October'] = processed_booking_cancel_df['arrival_date_month_October'].astype('int')
processed_booking_cancel_df['arrival_date_month_September'] = processed_booking_cancel_df['arrival_date_month_September'].astype('int')
processed_booking_cancel_df['meal_BB'] = processed_booking_cancel_df['meal_BB'].astype('int')
processed_booking_cancel_df['meal_FB'] = processed_booking_cancel_df['meal_FB'].astype('int')
processed_booking_cancel_df['meal_HB'] = processed_booking_cancel_df['meal_HB'].astype('int')
processed_booking_cancel_df['meal_SC'] = processed_booking_cancel_df['meal_SC'].astype('int')
processed_booking_cancel_df['reserved_room_type_A'] = processed_booking_cancel_df['reserved_room_type_A'].astype('int')
processed_booking_cancel_df['reserved_room_type_B'] = processed_booking_cancel_df['reserved_room_type_B'].astype('int')
processed_booking_cancel_df['reserved_room_type_C'] = processed_booking_cancel_df['reserved_room_type_C'].astype('int')
processed_booking_cancel_df['reserved_room_type_D'] = processed_booking_cancel_df['reserved_room_type_D'].astype('int')
processed_booking_cancel_df['reserved_room_type_E'] = processed_booking_cancel_df['reserved_room_type_E'].astype('int')
processed_booking_cancel_df['reserved_room_type_F'] = processed_booking_cancel_df['reserved_room_type_F'].astype('int')
processed_booking_cancel_df['reserved_room_type_G'] = processed_booking_cancel_df['reserved_room_type_G'].astype('int')

# Load Pretrained price_model using pickle
with open(cancel_model_dir, 'rb') as f:
    cancel_model = pickle.load(f)

# variables
predictor_cols = ['is_repeated_guest', 'arrival_date_month_April', 'arrival_date_month_August', 
                  'arrival_date_month_December', 'arrival_date_month_February', 'arrival_date_month_January', 
                  'arrival_date_month_July', 'arrival_date_month_June', 'arrival_date_month_March', 
                  'arrival_date_month_May', 'arrival_date_month_November', 'arrival_date_month_October', 
                  'arrival_date_month_September', 'meal_BB', 'meal_FB', 'meal_HB', 'meal_SC', 
                  'reserved_room_type_A', 'reserved_room_type_B', 'reserved_room_type_C', 
                  'reserved_room_type_D', 'reserved_room_type_E', 'reserved_room_type_F',
                  'reserved_room_type_G', 'market_segment_Aviation', 'market_segment_Complementary', 
                  'market_segment_Corporate', 'market_segment_Direct', 'market_segment_Groups', 
                  'market_segment_Offline TA/TO', 'market_segment_Online TA', 'lead_time', 
                  'stays_in_weekend_nights', 'stays_in_week_nights', 'adults', 'children', 
                  'previous_cancellations', 'previous_bookings_not_canceled', 'adr','required_car_parking_spaces',
                  'total_of_special_requests'
                 ]

predictors = processed_booking_cancel_df[predictor_cols]
predicted_cancel = cancel_model.predict(predictors)
processed_booking_cancel_df['predicted'] = predicted_cancel
processed_booking_cancel_df.rename(columns={'market_segment_Offline TA/TO': 'market_segment_Offline_TA_TO', 
                                            'market_segment_Online TA': 'market_segment_Online_TA'}, 
                                    inplace=True)

hotel_streaming_ml_cancel_csv = processed_booking_cancel_df.to_csv(index=False)
hotel_streaming_ml_cancel_csv_bytes = bytes(hotel_streaming_ml_cancel_csv, 'utf-8')
hotel_streaming_ml_cancel_csv_stream = io.BytesIO(hotel_streaming_ml_cancel_csv_bytes)

# Create a client object
client = bigquery.Client()

# Table ID
table_id = 'is3107-g2-381308.hotel_booking.hotel_booking_ml_cancel'

# Table Ref
table = client.get_table(table_id)

job_config = bigquery.LoadJobConfig(
    skip_leading_rows = 1,
    source_format = bigquery.SourceFormat().CSV,
    # Append    
    write_disposition = bigquery.WriteDisposition.WRITE_APPEND
)

# Append the streaming data to the table
job = client.load_table_from_file(hotel_streaming_ml_cancel_csv_stream, table_id, job_config=job_config)

job.result()