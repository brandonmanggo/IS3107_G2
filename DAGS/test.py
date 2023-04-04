import csv
import pandas as pd
from google.cloud import bigquery
import os
import io

ddir = '/Users/mellitaangga/Desktop/BZA/Y2S2/IS3107/IS3107_G2'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{ddir}/bigquery/is3107-g2-381308-b948b933d07a.json'

hotel_booking_dir = f'{ddir}/Dataset/batch_data/city_hotel_bookings_updated.csv'
hotel_booking_file = open(hotel_booking_dir)

hotel_booking_df = pd.read_csv(hotel_booking_file)

csv_reader = csv.reader(hotel_booking_df, delimiter=',')

eda_included_cols = ['Booking_ID', 'adults', 'children', 'stays_in_weekend_nights',
       'stays_in_week_nights', 'meal', 'required_car_parking_spaces',
       'reserved_room_type', 'lead_time',
       'arrival_date_month', 'arrival_date_day_of_month', 'market_segment',
       'is_repeated_guest', 'previous_cancellations',
       'previous_bookings_not_canceled', 'adr', 'total_of_special_requests',
       'is_canceled']

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