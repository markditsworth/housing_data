import pandas as pd
import datetime
import pymysql
from sqlalchemy import create_engine
from config.base import *

subset_columns = [
    "PROPERTY TYPE",
    "ADDRESS",
    "CITY",
    "STATE OR PROVINCE",
    "ZIP OR POSTAL CODE",
    "PRICE", 
    "BEDS", 
    "BATHS", 
    "LOCATION",
    "SQUARE FEET",
    "LOT SIZE", # null sometimes
    "YEAR BUILT", # null sometimes
    "DAYS ON MARKET", # null sometimes
    "$/SQUARE FEET",
    "HOA/MONTH",
    "SOURCE",
    "LATITUDE",
    "LONGITUDE"
]

# columns mappings
col_maps = {
    "PROPERTY TYPE": "PROPERTY_TYPE",
    "STATE OR PROVINCE": "STATE",
    "ZIP OR POSTAL CODE": "ZIP",
    "SQUARE FEET": "SQUARE_FEET",
    "LOT SIZE": "LOT_SIZE",
    "YEAR BUILT": "YEAR_BUILT",
    "DAYS ON MARKET": "DAYS_ON_MARKET",
    "$/SQUARE FEET": "PRICE_PER_SQUARE_FOOT",
    "HOA/MONTH": "HOA_PER_MONTH"
}

def transform(df):
    #subset of data and remap column names
    df = df[subset_columns].rename(columns=col_maps)
    #onyl first 5 digits of zip code
    df.loc[:,"ZIP"] = df["ZIP"].apply(lambda x: str(x).split("-")[0])
    # add date
    df["DATE"] = datetime.datetime.today().strftime('%Y-%m-%d')

    return df

def load(df, tablename="housing"):
    engine = f"mysql+pymysql://{mysql_server_username}:{mysql_server_password}@{mysql_server_address}:{mysql_server_port}/{mysql_db}"
    sqlEngine = create_engine(engine, pool_recycle=3600)
    dbConnection = sqlEngine.connect()
    frame = df.to_sql(mysql_table, dbConnection, index=False, if_exists='append')
