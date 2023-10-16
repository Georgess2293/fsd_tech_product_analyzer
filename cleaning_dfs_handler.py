
from logging_handler import show_error_message
#from pandas_handler import process_net_transfer_record, drop_nulls, fill_nulls
import pandas as pd
import misc_handler
from datetime import datetime



def replace_date(x):
    if "ago" in x:
        return datetime.now()
    else:
        return x

def clean_reviews_reddit(df):
    return_df=df.copy()
    if return_df.columns[0]=='Unnamed: 0': 
        return_df.pop(return_df.columns[0])
    return_df.columns=return_df.columns.str.replace(" ","_")
    return_df['Date']=pd.to_datetime(return_df['Date'])
    # columns=['product_id']
    # return_df[columns]=return_df[columns].astype('int64')
    return return_df


def clean_reviews_gsm(df):
    return_df=df.copy()
    return_df.drop(columns='User location', inplace=True)
    return_df.columns=return_df.columns.str.replace(" ","_")
    return_df['Date']=return_df['Date'].apply(replace_date)
    return_df['Date']=pd.to_datetime(return_df['Date'])
    columns=['Product_id']
    return_df[columns]=return_df[columns].astype('int64')
    return return_df

def clean_specs(df):
    return_df=df.copy()
    return_df.columns=return_df.columns.str.replace(" ","_")
    return_df.columns=return_df.columns.str.replace(".","_")
    return_df['Launch_Announced']=pd.to_datetime(return_df['Launch_Announced'])
    return_df.rename(columns={'Launch_Announced':'Release_Date'},inplace=True)   
    columns=['product_id']
    return_df[columns]=return_df[columns].astype('int64')
    return return_df





