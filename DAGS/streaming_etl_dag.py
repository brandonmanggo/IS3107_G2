from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from google.cloud import bigquery
import pandas as pd
import numpy as np
import pickle
from sklearn.preprocessing import StandardScaler


# Set the path to your service account key file
# Change the dir according to the location of the service account credential (is3107-g2-381308-b948b933d07a.json)
ddir = '/Users/mellitaangga/Desktop/BZA/Y2S2/IS3107/Project'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{ddir}/bigquery/is3107-g2-381308-b948b933d07a.json'

price_model_dir = ddir + '/Dashboard/Models/price_model.pkl'
cancel_model_dir = ddir + '/Dashboard/Models/cancel_model.pkl'

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
    
    preprocessed_booking_price_df = df.copy()
    
    ## Pre-Processing

    # Filling Missing Data for Children
    preprocessed_booking_price_df.children.fillna(0, inplace = True)
    preprocessed_booking_price_df['children'] = preprocessed_booking_price_df['children'].astype('int') 

    # Cleaning Data for adr
    preprocessed_booking_price_df.drop(index = [row for row in preprocessed_booking_price_df.index 
                                                if 400 < preprocessed_booking_price_df.loc[row, 'adr']], 
                                        inplace = True)
    preprocessed_booking_price_df.drop(index = [row for row in preprocessed_booking_price_df.index 
                                                if 0 >= preprocessed_booking_price_df.loc[row, 'adr']], 
                                        inplace = True)
    preprocessed_booking_price_df.adr = preprocessed_booking_price_df.adr.round()
    preprocessed_booking_price_df.adr = preprocessed_booking_price_df.adr.astype('int')

    ## One-Hot Encoding
    # For market_segment Column
    market_segment_one_hot = pd.get_dummies(preprocessed_booking_price_df['market_segment'], prefix='market_segment')
    preprocessed_booking_price_df = pd.concat([preprocessed_booking_price_df, market_segment_one_hot], axis=1)
    preprocessed_booking_price_df.drop('market_segment', axis=1, inplace=True)

    # For arrival_date_month Column
    arrival_date_month_one_hot = pd.get_dummies(preprocessed_booking_price_df['arrival_date_month'], prefix='arrival_date_month')
    preprocessed_booking_price_df = pd.concat([preprocessed_booking_price_df, arrival_date_month_one_hot], axis=1)
    preprocessed_booking_price_df.drop('arrival_date_month', axis=1, inplace=True)

    # For meal Column
    meal_one_hot = pd.get_dummies(preprocessed_booking_price_df['meal'], prefix='meal')
    preprocessed_booking_price_df = pd.concat([preprocessed_booking_price_df, meal_one_hot], axis=1)
    preprocessed_booking_price_df.drop('meal', axis=1, inplace=True)

    # For reserved_room_type Column
    reserved_room_type_one_hot = pd.get_dummies(preprocessed_booking_price_df['reserved_room_type'], prefix='reserved_room_type')
    preprocessed_booking_price_df = pd.concat([preprocessed_booking_price_df, reserved_room_type_one_hot], axis=1)
    preprocessed_booking_price_df.drop('reserved_room_type', axis=1, inplace=True)

    # Variables
    cols = ['lead_time', 'arrival_date_year', 'arrival_date_day_of_month',
            'stays_in_weekend_nights', 'stays_in_week_nights', 'adults', 
            'children', 'is_repeated_guest', 'previous_cancellations', 
            'previous_bookings_not_canceled', 'required_car_parking_spaces', 
            'total_of_special_requests', 'market_segment_Aviation', 
            'market_segment_Complementary', 'market_segment_Corporate', 
            'market_segment_Direct', 'market_segment_Groups', 
            'market_segment_Offline TA/TO', 'market_segment_Online TA', 
            'market_segment_Undefined', 'arrival_date_month_April',
            'arrival_date_month_August', 'arrival_date_month_December',
            'arrival_date_month_February', 'arrival_date_month_January',
            'arrival_date_month_July', 'arrival_date_month_June',
            'arrival_date_month_March', 'arrival_date_month_May',
            'arrival_date_month_November', 'arrival_date_month_October', 
            'arrival_date_month_September', 'meal_BB', 'meal_FB', 'meal_HB', 
            'meal_SC', 'reserved_room_type_A', 'reserved_room_type_B', 
            'reserved_room_type_C', 'reserved_room_type_D', 'reserved_room_type_E', 
            'reserved_room_type_F', 'reserved_room_type_G', 'Booking_ID',
            'is_canceled', 'adr' 
          ]
    
    ## Setting Data
    processed_booking_price_df = pd.DataFrame(columns=cols)
    processed_booking_price_df = pd.concat([processed_booking_price_df, preprocessed_booking_price_df])
    processed_booking_price_df.fillna(0, inplace = True)
    processed_booking_price_df.drop('is_canceled', axis=1, inplace=True)
    processed_booking_price_df['Booking_ID'] = processed_booking_price_df['Booking_ID'].str[3:]
    processed_booking_price_df['Booking_ID'] = processed_booking_price_df['Booking_ID'].astype('int') 
    processed_booking_price_df['market_segment_Aviation'] = processed_booking_price_df['market_segment_Aviation'].astype('int') 
    processed_booking_price_df['market_segment_Complementary'] = processed_booking_price_df['market_segment_Complementary'].astype('int') 
    processed_booking_price_df['market_segment_Corporate'] = processed_booking_price_df['market_segment_Corporate'].astype('int')
    processed_booking_price_df['market_segment_Direct'] = processed_booking_price_df['market_segment_Direct'].astype('int')
    processed_booking_price_df['market_segment_Groups'] = processed_booking_price_df['market_segment_Groups'].astype('int')
    processed_booking_price_df['market_segment_Offline TA/TO'] = processed_booking_price_df['market_segment_Offline TA/TO'].astype('int')
    processed_booking_price_df['market_segment_Online TA'] = processed_booking_price_df['market_segment_Online TA'].astype('int')
    processed_booking_price_df['market_segment_Undefined'] = processed_booking_price_df['market_segment_Undefined'].astype('int')
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

    ti.xcom_push('hotel_booking_price_df', processed_booking_price_df)
    

    # Data Preprocessing for Cancellation Prediction
    
    hotel_booking_cancel_df = df.copy()

    #Data cleaning to make 2015-2016 dataset match the incoming streaming data
    hotel_booking_cancel_df.loc[(hotel_booking_cancel_df['meal'] == 'Undefined'), 'meal']= 'SC'
    hotel_booking_cancel_df.loc[ (hotel_booking_cancel_df['required_car_parking_spaces'] > 1), 'required_car_parking_spaces']= 1

    #Filling in NA Values for Children column
    hotel_booking_cancel_df.children.fillna(0, inplace = True)
    

    #Dropping the arrival date variables (excludind month) which are not useful/relevant to the cancellation
    hotel_booking_cancel_df.drop(['arrival_date_year', 'arrival_date_day_of_month'], axis = 1, inplace = True)
    categorical_cols = ['arrival_date_month', 'meal', 'reserved_room_type', 'market_segment', 'is_repeated_guest']
    numerical_cols = ['lead_time', 'stays_in_weekend_nights', 'stays_in_week_nights', 'adults', 'children', 'previous_cancellations', 'previous_bookings_not_canceled',
                  'adr', 'required_car_parking_spaces', 'total_of_special_requests']
    numerical_df = hotel_booking_cancel_df.drop(categorical_cols, axis = 1)
    numerical_df.drop(['is_canceled'], axis = 1, inplace=True)
    categorical_df = hotel_booking_cancel_df.drop(numerical_cols, axis = 1)

    #Encoding categorical variables
    # For market_segment Column
    market_segment_one_hot = pd.get_dummies(categorical_df['market_segment'], prefix='market_segment')
    categorical_df = pd.concat([categorical_df, market_segment_one_hot], axis=1)
    categorical_df.drop('market_segment', axis=1, inplace=True)

    # For arrival_date_month Column
    arrival_date_month_one_hot = pd.get_dummies(categorical_df['arrival_date_month'], prefix='arrival_date_month')
    categorical_df = pd.concat([categorical_df, arrival_date_month_one_hot], axis=1)
    categorical_df.drop('arrival_date_month', axis=1, inplace=True)

    # For meal Column
    meal_one_hot = pd.get_dummies(categorical_df['meal'], prefix='meal')
    categorical_df = pd.concat([categorical_df, meal_one_hot], axis=1)
    categorical_df.drop('meal', axis=1, inplace=True)

    # For reserved_room_type Column
    reserved_room_type_one_hot = pd.get_dummies(categorical_df['reserved_room_type'], prefix='reserved_room_type')
    categorical_df = pd.concat([categorical_df, reserved_room_type_one_hot], axis=1)
    categorical_df.drop('reserved_room_type', axis=1, inplace=True)

    #Scaling of Numerical Variables
    categorical_var = hotel_booking_cancel_df[categorical_cols]
    scaler = StandardScaler()
    numerical_var_scaled = pd.DataFrame(scaler.fit_transform(numerical_df), columns=numerical_var.columns)
    hotel_booking_cancel_df = pd.concat([categorical_var, numerical_var_scaled], axis = 1)

    ti.xcom_push('hotel_booking_cancel_df', hotel_booking_cancel_df)




def predict_price(**kwargs):
    ti = kwargs['ti']

    booking_price_df = ti.xcom_pull(task_ids = 'transform', key = 'hotel_booking_price_df')

    ## Load Pretrained price_model using pickle
    with open(price_model_dir, 'rb') as f:
        price_model = pickle.load(f)

    # Predict the price for the customer
    predictors_cols = ['lead_time', 'arrival_date_year', 'arrival_date_day_of_month',
                        'stays_in_weekend_nights', 'stays_in_week_nights', 'adults', 
                        'children', 'is_repeated_guest', 'previous_cancellations', 
                        'previous_bookings_not_canceled', 'required_car_parking_spaces', 
                        'total_of_special_requests', 'market_segment_Aviation', 
                        'market_segment_Complementary', 'market_segment_Corporate', 
                        'market_segment_Direct', 'market_segment_Groups', 
                        'market_segment_Offline TA/TO', 'market_segment_Online TA', 
                        'market_segment_Undefined', 'arrival_date_month_April',
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
    predictors = booking_price_df[predictors_cols]
    predicted_price = price_model.predict(predictors)

    # Append the predicted price back to original df
    booking_price_df['predicted'] = predicted_price
    booking_price_df.rename(columns={'market_segment_Offline TA/TO': 'market_segment_Offline_TA_TO', 
                                     'market_segment_Online TA': 'market_segment_Online_TA'}, 
                            inplace=True)

    ti.xcom_push('hotel_booking_price_csv', booking_price_df.to_csv(index = False))



def predict_cancel(**kwargs):
    ti = kwargs['ti']

    booking_cancel_df = ti.xcom_pull(task_ids = 'transform', key = 'hotel_booking_cancel_df')
    hotel_booking_df = ti.xcom_pull(task_ids = 'transform', key = 'hotel_booking_df')

    # Load Pretrained price_model using pickle
    with open(cancel_model_dir, 'wb') as f:
        cancel_model = pickle.load(f)

    # variables
    predictor_cols = ['arrival_date_month_April', 'arrival_date_month_August', 'arrival_date_month_December', 
                      'arrival_date_month_February', 'arrival_date_month_January', 'arrival_date_month_July',
                      'arrival_date_month_June', 'arrival_date_month_March', 'arrival_date_month_May',
                      'arrival_date_month_November', 'arrival_date_month_October', 'arrival_date_month_September',
                      'meal_BB', 'meal_FB', 'meal_HB', 'meal_SC', 'reserved_room_type_A', 'reserved_room_type_B',
                      'reserved_room_type_C', 'reserved_room_type_D', 'reserved_room_type_E', 'reserved_room_type_F',
                      'reserved_room_type_G', 'reserved_room_type_H', 'reserved_room_type_L', 'reserved_room_type_P',
                      'market_segment_Aviation', 'market_segment_Complementary', 'market_segment_Corporate', 'market_segment_Direct',
                      'market_segment_Groups', 'market_segment_Offline TA/TO', 'market_segment_Online TA', 'market_segment_Undefined',
                      'is_repeated_guest_0', 'is_repeated_guest_1', 'lead_time', 'stays_in_weekend_nights', 'stays_in_week_nights',
                      'adults', 'children', 'previous_cancellations', 'previous_bookings_not_canceled', 'adr','required_car_parking_spaces',
                      'total_of_special_requests'
                      ]
    
    predictors = booking_cancel_df[predictor_cols]
    # predict the cancellation of the booking based on the above variables
    predicted_cancel = cancel_model.predict(predictors)

    # Append the predicted price back to original df
    hotel_booking_df['predicted'] = predicted_cancel

    ti.xcom_push('hotel_booking_cancel_csv', hotel_booking_df.to_csv(index = False))
    


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