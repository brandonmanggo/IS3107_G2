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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 26),
}

dag = DAG(
    'quarterly_etl',
    default_args=default_args,
    description='My DAG with parameter',
    schedule_interval=None,
)

def extract(**kwargs):
    ti = kwargs['ti']
    year = kwargs.get('year')
    quarter = kwargs.get('quarter')

    month_dict = {
        1: ['January', 'February', 'March'],
        2: ['April', 'May', 'June'],
        3: ['July', 'August', 'September'],
        4: ['October', 'November', 'December']
    }

    # Set up BigQuery client 
    client = bigquery.Client()

    # Table ID
    table_id_price = 'is3107-g2-381308.hotel_booking.hotel_booking_ml_price'
    table_id_cancel = 'is3107-g2-381308.hotel_booking.hotel_booking_ml_cancel'

    # Query quarterly data 
    query_price = f"""
    SELECT *
    FROM `{table_id_price}`
    WHERE arrival_date_month IN ({','.join(['"' + str(val) + '"' for val in month_dict[quarter]])})
    AND arrival_date_year = `{year}`
    """

    query_cancel = f"""
    SELECT *
    FROM `{table_id_cancel}`
    WHERE arrival_date_month IN ({','.join(['"' + str(val) + '"' for val in month_dict[quarter]])})
    AND arrival_date_year = `{year}`
    """

    query_price_all = f"""
    SELECT *
    FROM `{table_id_price}`
    """

    query_cancel_all = f"""
    SELECT *
    FROM `{table_id_cancel}`
    """

    output_dir_price = f'{ddir}/output/{year}-Q{quarter}-price.csv' 
    output_dir_cancel = f'{ddir}/output/{year}-Q{quarter}-cancel.csv' 
        
    query_job_price = client.query(query_price)
    query_job_cancel = client.query(query_cancel)

    result_price = query_job_price.result()
    result_cancel = query_job_cancel.result()

    with open(output_dir_price, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([field.name for field in results.schema])
        for row in result_price:
            writer.writerow(row)
    
    with open(output_dir_cancel, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([field.name for field in results.schema])
        for row in result_price:
            writer.writerow(row)
    
    query_job_price_all = client.query(query_price_all)
    qury_job_cancel_all = client.query(query_cancel_all)

    result_price_all = query_job_price_all.result()
    result_cancel_all = query_job_cancel_all.result()

    booking_price_df = result_price_all.to_dataframe()
    booking_cancel_df = result_cancel_all.to_dataframe()

    ti.xcom_push('booking_price_df', booking_price_df)
    ti.xcom_push('booking_cancel_df', booking_cancel_df)

def update_price_model(**kwargs):
    ti = kwargs['ti']

    hotel_price_df = ti.xcom_pull(task_ids= 'extract', key= 'booking_price_df')

    # train model 
    #KM

    # Save updated model
    price_model_dir = f'{ddir}/models/price_model.pkl'
    with open(price_model_dir, "wb") as f:
        pickle.dump(model, f)


def update_cancel_model(**kwargs):
    ti = kwargs['ti']

    hotel_cancel_df = ti.xcom_pull(task_ids= 'extract', key= 'booking_cancel_df')

    # train model 
    #JY

    # Save updated model
    cancel_model_dir = f'{ddir}/models/cancel_model.pkl'
    with open(cancel_model_dir, "wb") as f:
        pickle.dump(model, f)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

update_price_model = PythonOperator(
    task_id='update_price_model',
    python_callable=update_price_model,
    dag=dag,
)

update_cancel_model = PythonOperator(
    task_id='update_cancel_model',
    python_callable=update_cancel_model,
    dag=dag,
)
    
extract >> [update_price_model, update_cancel_model]




