from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from google.cloud import bigquery
import pandas as pd
import numpy as np
import pickle
import db_dtypes
from io import StringIO

from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import AdaBoostClassifier

import lightgbm as ltb
import xgboost as xgb
import catboost as cb

# Set the path to your service account key file
# Change the dir according to the location of the service account credential (is3107-g2-381308-b948b933d07a.json)

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

    ddir = '/Users/mellitaangga/Desktop/BZA/Y2S2/IS3107/Project'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{ddir}/bigquery/is3107-g2-381308-b948b933d07a.json'

    # Set up BigQuery client 
    client = bigquery.Client()

    # Table ID
    table_id_price = 'is3107-g2-381308.hotel_booking.hotel_booking_ml_price'
    table_id_cancel = 'is3107-g2-381308.hotel_booking.hotel_booking_ml_cancel'

    # Query quarterly data 
    query_price = f"""
    SELECT *
    FROM `{table_id_price}`
    WHERE arrival_date_month_`{month_dict[quarter][0]}` = 1
    AND arrival_date_month_`{month_dict[quarter][1]}` = 1
    AND arrival_date_month_`{month_dict[quarter][2]}` = 1
    AND arrival_date_year = 2017
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

    quarterly_price_df = result_price.to_dataframe()
    quarterly_cancel_df = result_cancel.to_dataframe()
    
    query_job_price_all = client.query(query_price_all)
    query_job_cancel_all = client.query(query_cancel_all)

    result_price_all = query_job_price_all.result()
    result_cancel_all = query_job_cancel_all.result()

    booking_price_df = result_price_all.to_dataframe()
    booking_cancel_df = result_cancel_all.to_dataframe()

    ti.xcom_push('booking_price_df', booking_price_df.to_csv(index=False))
    ti.xcom_push('booking_cancel_df', booking_cancel_df.to_csv(index=False))

def update_price_model(**kwargs):
    ti = kwargs['ti']

    hotel_price_csv = ti.xcom_pull(task_ids= 'extract', key= 'booking_price_df')
    hotel_price_df = pd.read_csv(StringIO(hotel_price_csv))
    # Splitting Data (80:20) Regression
    x = hotel_price_df.drop(columns = ['adr', 'predicted'])
    y = hotel_price_df.adr 
    x_train, x_test, y_train, y_test = train_test_split(x, y, train_size=0.8, test_size=0.2, random_state=42)

    # Train Models
    rfr = RandomForestRegressor()
    rfr_model = rfr.fit(x_train, y_train)
    rfr_y_pred = rfr_model.predict(x_test)
    rfr_r2 = r2_score(y_test, rfr_y_pred)

    new_price_model = rfr_model
    new_r2 = rfr_r2

    gbr = GradientBoostingRegressor()
    gbr_model = gbr.fit(x_train, y_train)
    gbr_y_pred = gbr_model.predict(x_test)
    gbr_r2 = r2_score(y_test, gbr_y_pred)

    if (gbr_r2 > new_r2):
        new_r2 = gbr_r2
        new_price_model = gbr_model
    
    lgbmr = ltb.LGBMRegressor()
    lgbmr_model = lgbmr.fit(x_train, y_train)
    lgbmr_y_pred = lgbmr_model.predict(x_test)
    lgbmr_r2 = r2_score(y_test, lgbmr_y_pred)

    if (lgbmr_r2 > new_r2):
        new_r2 = lgbmr_r2
        new_price_model = lgbmr_model
    
    xgbr = xgb.XGBRegressor()
    xgbr_model = xgbr.fit(x_train, y_train)
    xgbr_y_pred = xgbr_model.predict(x_test)
    xgbr_r2 = r2_score(y_test, xgbr_y_pred)

    if (xgbr_r2 > new_r2):
        new_r2 = xgbr_r2
        new_price_model = xgbr_model

    cbr = cb.CatBoostRegressor()
    cbr_model = cbr.fit(x_train, y_train)
    cbr_y_pred = cbr_model.predict(x_test)
    cbr_r2 = r2_score(y_test, cbr_y_pred)

    if (cbr_r2 > new_r2):
        new_r2 = cbr_r2
        new_price_model = cbr_model

    ddir = '/Users/mellitaangga/Desktop/BZA/Y2S2/IS3107/Project'

    # Save Updated Model
    price_model_dir = f'{ddir}/Dashboard/Models/price_model.pkl'
    with open(price_model_dir, "wb") as f:
        pickle.dump(new_price_model, f)


def update_cancel_model(**kwargs):
    ti = kwargs['ti']

    hotel_cancel_df = ti.xcom_pull(task_ids= 'extract', key= 'booking_cancel_df')

    # train model 
    #JY
    # Splitting Data (80:20) Regression
    x = hotel_cancel_df.drop(columns = 'is_canceled')
    y = hotel_cancel_df['is_canceled'] 
    x_train, x_test, y_train, y_test = train_test_split(x, y, train_size=0.8, test_size=0.2, random_state=42)
    
    #Train Models
    #Random Forest Classifier
    rfr = RandomForestRegressor()
    rfr_model = rfr.fit(x_train, y_train)
    rfr_y_pred = rfr_model.predict(x_test)
    rfr_r2 = r2_score(y_test, rfr_y_pred)

    new_cancel_model = rfr_model
    new_r2 = rfr_r2

    #Cat Boost Classifier
    cbr = cb.CatBoostClassifier(iterations = 100)
    cbr_model = cbr.fit(x_train, y_train)
    cbr_y_pred = cbr_model.predict(x_test)
    cbr_r2 = r2_score(y_test, cbr_y_pred)

    if (cbr_r2 > new_r2): 
        new_r2 = cbr_r2
        new_cancel_model = cbr_model

    #K Nearest Neighbours Classifier
    knn = KNeighborsClassifier()
    knn_model = knn.fit(x_train, y_train)
    knn_y_pred = knn_model.predict(x_test)
    knn_r2 = r2_score(y_test, knn_y_pred)

    if (knn_r2 > new_r2):
        new_r2 = knn
        new_cancel_model = knn_model

    #Gradient Bosst Classifier
    gb = GradientBoostingClassifier()
    gb_model = gb.fit(x_train, y_train)
    gb_y_pred = gb_model.predict(x_test)
    gb_r2 = r2_score(y_test, gb_y_pred)

    if(gb_r2 > new_r2): 
        new_r2 = gb_r2
        new_cancel_model = gb_model
    
    
    # Save updated model
    cancel_model_dir = f'{ddir}/models/cancel_model.pkl'
    with open(cancel_model_dir, "wb") as f:
        pickle.dump(new_cancel_model, f)

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




