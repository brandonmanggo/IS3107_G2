from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from google.cloud import bigquery
import pandas as pd
import numpy as np
import pickle


# Set the path to your service account key file
# Change the dir according to the location of the service account credential (is3107-g2-381308-b948b933d07a.json)
ddir = '/Users/mellitaangga/Desktop/BZA/Y2S2/IS3107/Project'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{ddir}/bigquery/is3107-g2-381308-b948b933d07a.json'

price_model_dir = ddir + 'models/price_model.pkl'
cancel_model_dir = ddir + 'models/cancel_model.pkl'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 26),
}

dag = DAG(
    'streaming_etl',
    default_args=default_args,
    description='My DAG with parameter',
    schedule_interval=None,
)

def transform(**kwargs):
    ti = kwargs['ti']
    data = kwargs.get('data')
    df = pd.read_csv(data)
    
    # Data Preprocessing for Price Prediction
    #KM
    ti.xcom_push('hotel_booking_price_df', processed_booking_price_df)
    

    # Data Preprocessing for Cancellation Prediction
    #JY
    ti.xcom_push('hotel_booking_cancel_df', hotel_booking_cancel_df)

    
    ti.xcom_push('hotel_booking_df', hotel_booking_df)



def predict_price(**kwargs):
    ti = kwargs['ti']

    booking_price_df = ti.xcom_pull(task_ids = 'transform', key = 'hotel_booking_price_df')
    hotel_booking_df = ti.xcom_pull(task_ids = 'transform', key = 'hotel_booking_df')

    # Load Pretrained price_model using pickle
    with open(price_model_dir, 'wb') as f:
        price_model = pickle.load(f)

    # variables
    predictors = booking_price_df.iloc[:,:-1]

    # predict the price for the customer according to lead time, type of room, 
    # and additional booking details
    predicted_price = price_model.predict(predictors)

    # Append the predicted price back to original df
    hotel_booking_df['predicted'] = predicted_price

    ti.xcom_push('hotel_booking_price_csv', hotel_booking_df.to_csv())



def predict_cancel(**kwargs):
    ti = kwargs['ti']

    booking_cancel_df = ti.xcom_pull(task_ids = 'transform', key = 'hotel_booking_cancel_df')
    hotel_booking_df = ti.xcom_pull(task_ids = 'transform', key = 'hotel_booking_df')

    # Load Pretrained price_model using pickle
    with open(cancel_model_dir, 'wb') as f:
        cancel_model = pickle.load(f)

    # variables
    predictors = booking_cancel_df.iloc[:,:-1]

    # predict the price for the customer according to lead time, type of room, 
    # and additional booking details
    predicted_cancel = cancel_model.predict(predictors)

    # Append the predicted price back to original df
    hotel_booking_df['predicted'] = predicted_cancel

    ti.xcom_push('hotel_booking_cancel_csv', hotel_booking_df.to_csv())
    
def load_price(**kwargs):
    ti = kwargs['ti']
    hotel_booking_price_csv = ti.xcom_pull(task_ids = 'predict_price', key = 'hotel_booking_price_csv')

    hotel_booking_price_csv_bytes = bytes(hotel_booking_price_csv, 'utf-8')
    hotel_booking_price_csv_stream = io.BytesIO(hotel_booking_price_csv_bytes)

    # Set up BigQuery client 
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
    job = client.load_table_from_file(hotel_booking_price_csv_stream, table_id, job_config=job_config)

    job.result()



def load_cancel(**kwargs):
    ti = kwargs['ti']
    hotel_booking_cancel_csv = ti.xcom_pull(task_ids = 'predict_cancel', key = 'hotel_booking_cancel_csv')

    hotel_booking_cancel_csv_bytes = bytes(hotel_booking_cancel_csv, 'utf-8')
    hotel_booking_cancel_csv_stream = io.BytesIO(hotel_booking_cancel_csv_bytes)

    # Set up BigQuery client 
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
    job = client.load_table_from_file(hotel_booking_cancel_csv_stream, table_id, job_config=job_config)

    job.result()

transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

predict_price = PythonOperator(
    task_id='predict_price',
    python_callable=predict_price,
    dag=dag,
)

predict_cancel = PythonOperator(
    task_id='predict_cancel',
    python_callable=predict_cancel,
    dag=dag,
)

load_price = PythonOperator(
    task_id='load_price',
    python_callable=load_price,
    dag=dag,
)

load_cancel = PythonOperator(
    task_id='load_cancel',
    python_callable=load_cancel,
    dag=dag,
)

transform >> [predict_price, predict_cancel] >> [load_price, load_cancel]