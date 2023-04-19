from google.cloud import bigquery
import os
import pickle
import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
from sklearn.metrics import accuracy_score
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import GradientBoostingClassifier

import lightgbm as ltb
import xgboost as xgb
import catboost as cb

# Set the path to your service account key file
ddir = 'C:\\Users\\KMwong\\Desktop\\IS3107\\Projects\\IS3107_G2'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{ddir}\\BigQuery\\is3107-g2-381308-b948b933d07a.json'

#### Run Job By Job To Test (Else rename job with job+number)

### For Quarterly ETL DAG

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

## Query quarterly data 
# query_price = f"""
# SELECT *
# FROM `{table_id_price}`
# WHERE arrival_date_month_`{month_dict[quarter][0]}` = 1
# OR arrival_date_month_`{month_dict[quarter][1]}` = 1
# OR arrival_date_month_`{month_dict[quarter][2]}` = 1
# AND arrival_date_year = 2017
# """

# query_cancel = f"""
# SELECT *
# FROM {table_id_cancel}
# WHERE arrival_date_month_{month_dict[quarter][0]} = 1
# OR arrival_date_month_{month_dict[quarter][1]} = 1
# OR arrival_date_month_{month_dict[quarter][2]} = 1
# AND arrival_date_year = 2017
# """

query_price = f"""
# SELECT *
# FROM `{table_id_price}`
# WHERE arrival_date_month_July = 1
# AND arrival_date_year = 2017
 """

query_cancel = f"""
# SELECT *
# FROM `{table_id_cancel}`
# WHERE arrival_date_month_July = 1
# AND arrival_date_year = 2017
 """

query_price_all = f"""
SELECT *
FROM `{table_id_price}`
"""

query_cancel_all = f"""
SELECT *
FROM `{table_id_cancel}`
"""

output_dir_price = f'{ddir}\\output\\2017-Q3-price.csv'  
output_dir_cancel = f'{ddir}\\output\\2017-Q3-cancel.csv'

## For Price
query_job_price = client.query(query_price)
result_price = query_job_price.result()
quarterly_price_df = result_price.to_dataframe()

query_job_price_all = client.query(query_price_all)
result_price_all = query_job_price_all.result()
booking_price_df = result_price_all.to_dataframe()

booking_price_df.drop('Booking_ID', axis=1, inplace=True)
booking_price_df.rename(columns={'market_segment_Offline_TA_TO': 'market_segment_Offline TA/TO', 
                                 'market_segment_Online_TA': 'market_segment_Online TA'}, 
                        inplace=True)

# cols = [field.name for field in result_price.schema]
predictors_cols = ['lead_time', 'arrival_date_year',
                    'stays_in_weekend_nights', 'stays_in_week_nights', 'adults', 
                    'children', 'is_repeated_guest', 'previous_cancellations', 
                    'previous_bookings_not_canceled', 'required_car_parking_spaces', 
                    'total_of_special_requests', 'market_segment_Aviation', 
                    'market_segment_Complementary', 'market_segment_Corporate', 
                    'market_segment_Direct', 'market_segment_Groups', 
                    'market_segment_Offline TA/TO', 'market_segment_Online TA', 
                        'arrival_date_month_April',
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
    
hotel_price_df_ordered = booking_price_df[predictors_cols]

# Splitting Data (80:20) Regression
x = hotel_price_df_ordered
y = booking_price_df.adr 
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

print(new_r2)
print(new_price_model)

# Save Updated Model
price_model_dir = f'{ddir}\\Dashboard\\Models\\price_model.pkl'
with open(price_model_dir, "wb") as f:
    pickle.dump(new_price_model, f)


## For Cancellation 

query_job_cancel = client.query(query_cancel)
result_cancel = query_job_cancel.result()
quarterly_cancel_df = result_cancel.to_dataframe()

query_job_cancel_all = client.query(query_cancel_all)
result_cancel_all = query_job_cancel_all.result()
booking_cancel_df = result_cancel_all.to_dataframe()

booking_cancel_df.drop('Booking_ID', axis=1, inplace=True)
booking_cancel_df.rename(columns={'market_segment_Offline_TA_TO': 'market_segment_Offline TA/TO', 
                                  'market_segment_Online_TA': 'market_segment_Online TA'}, 
                         inplace=True)

predictor_cols = ['is_repeated_guest', 'arrival_date_month_April', 'arrival_date_month_August', 'arrival_date_month_December', 
                    'arrival_date_month_February', 'arrival_date_month_January', 'arrival_date_month_July',
                    'arrival_date_month_June', 'arrival_date_month_March', 'arrival_date_month_May',
                    'arrival_date_month_November', 'arrival_date_month_October', 'arrival_date_month_September',
                    'meal_BB', 'meal_FB', 'meal_HB', 'meal_SC', 'reserved_room_type_A', 'reserved_room_type_B',
                    'reserved_room_type_C', 'reserved_room_type_D', 'reserved_room_type_E', 'reserved_room_type_F',
                    'reserved_room_type_G', 'market_segment_Aviation', 'market_segment_Complementary', 'market_segment_Corporate', 
                    'market_segment_Direct', 'market_segment_Groups', 'market_segment_Offline TA/TO', 'market_segment_Online TA',
                    'lead_time', 'stays_in_weekend_nights', 'stays_in_week_nights', 'adults', 'children', 'previous_cancellations', 
                    'previous_bookings_not_canceled', 'adr','required_car_parking_spaces', 'total_of_special_requests'
                 ]

hotel_cancel_df_ordered = booking_cancel_df[predictor_cols] 

# Splitting Data (80:20) Classification
x = hotel_cancel_df_ordered
y = booking_cancel_df.is_canceled
x_train, x_test, y_train, y_test = train_test_split(x, y, train_size=0.8, test_size=0.2, random_state=42)

# Random Forest Classifier
rfc = RandomForestClassifier()
rfc_model = rfc.fit(x_train, y_train)
rfc_y_pred = rfc_model.predict(x_test)
rfc_acc = accuracy_score(y_test, rfc_y_pred)

new_cancel_model = rfc_model
new_acc = rfc_acc

#Cat Boost Classifier
cbc = cb.CatBoostClassifier(iterations = 100)
cbc_model = cbc.fit(x_train, y_train)
cbc_y_pred = cbc_model.predict(x_test)
cbc_acc = accuracy_score(y_test, cbc_y_pred)

if (cbc_acc > new_acc): 
    new_acc = cbc_acc
    new_cancel_model = cbc_model

# K Nearest Neighbours Classifier
knn = KNeighborsClassifier()
knn_model = knn.fit(x_train, y_train)
knn_y_pred = knn_model.predict(x_test)
knn_acc = accuracy_score(y_test, knn_y_pred)

if (knn_acc > new_acc): 
    new_acc = knn_acc
    new_cancel_model = knn_model

# Gradient Boost Classifier
gbc = GradientBoostingClassifier()
gbc_model = gbc.fit(x_train, y_train)
gbc_y_pred = gbc_model.predict(x_test)
gbc_acc = accuracy_score(y_test, gbc_y_pred)

if (gbc_acc > new_acc): 
    new_acc = gbc_acc
    new_cancel_model = gbc_model

print(new_acc)
print(new_cancel_model)

# Save Updated Model
cancel_model_dir = f'{ddir}\\Dashboard\\Models\\cancellation_model.pkl'
with open(cancel_model_dir, "wb") as f:
    pickle.dump(new_cancel_model, f)