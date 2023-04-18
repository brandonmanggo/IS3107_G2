from google.cloud import bigquery
import os
import io
import csv
import pickle
import numpy as np
import pandas as pd
import db_dtypes

from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import RandomForestRegressor
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

## Query quarterly data 
# query_price = f"""
# SELECT *
# FROM `{table_id_price}`
# WHERE arrival_date_month_`{month_dict[quarter][0]}` = 1
# AND arrival_date_month_`{month_dict[quarter][1]}` = 1
# AND arrival_date_month_`{month_dict[quarter][2]}` = 1
# AND arrival_date_year = 2017
# """

query_price = f"""
# SELECT *
# FROM `{table_id_price}`
# WHERE arrival_date_month_July = 1
# AND arrival_date_year = 2017
 """

query_price_all = f"""
SELECT *
FROM `{table_id_price}`
"""

output_dir_price = f'{ddir}\\output\\2017-Q3-price.csv' 
query_job_price = client.query(query_price)
result_price = query_job_price.result()

df = result_price.to_dataframe()

print(df)

query_job_price_all = client.query(query_price_all)
result_price_all = query_job_price_all.result()

cols = [field.name for field in result_price.schema]

booking_price_df = result_price_all.to_dataframe()
hotel_price_df = booking_price_df.copy()

print(hotel_price_df.head())
print(hotel_price_df.iloc[0].shape)

print(hotel_price_df.info())
 # Splitting Data (80:20) Regression
x = hotel_price_df.drop(columns = ['adr','predicted'])
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

print(new_r2)
print(new_price_model)

# Save Updated Model
price_model_dir = f'{ddir}\\Dashboard\\Models\\price_model.pkl'
with open(price_model_dir, "wb") as f:
    pickle.dump(new_price_model, f)