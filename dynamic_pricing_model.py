import numpy as np
import pandas as pd
import xgboost as xg
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

url = (
    'https://raw.githubusercontent.com/``dgmitrzuk/dynamic-pricing-Uber-Kafka/'
    'refs/heads/main/dynamic_pricing.csv'
)
df = pd.read_csv(url)

y = df['Historical_Cost_of_Ride']
X = pd.get_dummies(df.drop(columns=['Historical_Cost_of_Ride']), drop_first=True)

train_X, test_X, train_y, test_y = train_test_split(X, y, test_size=0.3, random_state=123)

xgb_r = xg.XGBRegressor(
    objective='reg:squarederror',
    n_estimators=300,
    learning_rate=0.1,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8,
    seed=123,
)

xgb_r.fit(train_X, train_y)



MODEL_FEATURES = train_X.columns.tolist()


def dynamic_price_predict(df_input):
    df_enc = pd.get_dummies(df_input, drop_first=True)
    df_enc = df_enc.reindex(columns=MODEL_FEATURES, fill_value=0)
    return xgb_r.predict(df_enc)