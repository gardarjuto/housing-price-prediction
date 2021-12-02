from .config import *

import pandas as pd
import matplotlib.pyplot as plt
import sklearn.decomposition as decomposition
import numpy as np


def query_prices_latlon_date(conn, min_lat, max_lat, min_lon, max_lon, start_date, end_date):
    """
    Queries database for all datapoints within latitude and longitude range and the start and end date.
    Joins the period within the Price Paid table with the postcode table.
    Converts the rows into a pandas DataFrame and returns it. Irrelevant columns are excluded such as unique ids and object 
    names since they don't have predictive power.
    """
    sql = f"""
    SELECT price, date_of_transfer, property_type, latitude, longitude
    FROM (
        SELECT postcode, price, date_of_transfer, property_type, new_build_flag, tenure_type, street, locality, town_city, district, county, ppd_category_type, record_status
        FROM property_prices.pp_data
	    WHERE date_of_transfer BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
        ) pp 
    JOIN property_prices.postcode_data pd 
    ON pd.postcode = pp.postcode
    WHERE latitude >= {min_lat} AND latitude <= {max_lat} AND longitude >= {min_lon} AND longitude <= {max_lon};
    """
    df = pd.read_sql(sql, conn)
    return df


def clean_prices_df(df):
    """Cleans dataframe by filtering out rows with missing values and converting price to float and date to datetime format."""
    df_clean = df.dropna()
    df_clean.price = df_clean.price.astype(np.float64)
    df_clean.date_of_transfer = pd.to_datetime(df_clean.date_of_transfer)
    return df_clean


def one_hot_encode_cols(df):
    """Returns a dataframe with all columns one-hot encoded."""
    return pd.concat([pd.get_dummies(df[col], prefix=col, dummy_na=True) for col in df.columns], axis=1)


def print_correlations(df):
    """Calculates and prints the correlations between price and columns of type float"""
    for col in df.select_dtypes(include='float').columns:
        if col != 'price':
            print(f"Correlation between pricea and {col}:", df['price'].corr(df[col]))


