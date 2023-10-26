from database_handler import execute_query, create_connection, close_connection,return_data_as_df, return_insert_into_sql_statement_from_df
from lookups import InputTypes, IncrementalField,ETLStep,DESTINATION_SCHEMA
from datetime import datetime
# from prehook import return_tables_by_schema, return_lookup_items_as_dict, execute_sql_folder
import misc_handler
import cleaning_dfs_handler
import pandas as pd
import praw
import os
from time import sleep
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException,WebDriverException
from selenium.webdriver.common.action_chains import ActionChains
    


def execute_hook_sql(db_session, sql_command_directory_path):
    sql_files =misc_handler.retreive_sql_files(sql_command_directory_path)
    for sql_file in sql_files:
        if str(sql_file.split('-')[0].split('_')[1]) == ETLStep.HOOK.value:
            with open(os.path.join(sql_command_directory_path,sql_file), 'r') as file:
                sql_query = file.read()
                sql_query = sql_query.replace('target_schema', DESTINATION_SCHEMA.DESTINATION_NAME.value)
                execute_query(db_session, sql_query)


def insert_specs_gsm_reviews_stg(db_session,reddit,all_reviews_reddit,driver,all_specs=None,all_reviews=None):
    all_specs,all_reviews=misc_handler.return_stg_specs_exception_df(driver)
    all_specs=cleaning_dfs_handler.clean_specs(all_specs)
    all_reviews=cleaning_dfs_handler.clean_reviews_gsm(all_reviews)
    all_reviews=misc_handler.sentiment_analysis_df(all_reviews)
    insert_stmt_specs=return_insert_into_sql_statement_from_df(all_specs,'stg_products_specs')
    insert_stmt_reviews=return_insert_into_sql_statement_from_df(all_reviews,'stg_gsm_reviews')
    for insert in insert_stmt_specs:
        execute_query(db_session=db_session,query=insert)
    for insert in insert_stmt_reviews:
        execute_query(db_session=db_session,query=insert)
    all_reviews_reddit=misc_handler.return_all_reddit_df(all_specs,reddit)
    all_reviews_reddit=cleaning_dfs_handler.clean_reviews_reddit(all_reviews_reddit)
    all_reviews_reddit=misc_handler.sentiment_analysis_df(all_reviews_reddit)
    insert_stmt_reviews_reddit=return_insert_into_sql_statement_from_df(all_reviews_reddit,'stg_reddit_reviews')
    for insert in insert_stmt_reviews_reddit:
        execute_query(db_session=db_session,query=insert)


def create_etl_last_date_gsm(schema_name , db_session):
    query = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.etl_last_date_gsm
        (
            product_id INT UNIQUE,
            etl_last_date DATE
        )
        """
    execute_query(db_session, query)

def create_etl_last_date_reddit(schema_name , db_session):
    query = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.etl_last_date_reddit
        (
            product_id INT UNIQUE,
            etl_last_date DATE
        )
        """
    execute_query(db_session, query)
    

def return_max_date_gsm(product_id,schema_name,db_session):
    query=f"""SELECT etl_last_date FROM {schema_name}.etl_last_date_gsm WHERE product_id={product_id}"""
    return_df=return_data_as_df(query,input_type=InputTypes.SQL,db_session=db_session)
    return return_df

def return_max_date_reddit(product_id,schema_name,db_session):
    query=f"""SELECT etl_last_date FROM {schema_name}.etl_last_date_reddit WHERE product_id={product_id}"""
    return_df=return_data_as_df(query,input_type=InputTypes.SQL,db_session=db_session)
    return return_df

def update_last_date_gsm(schema_name,db_session):
    query=f"""
        INSERT INTO {schema_name}.etl_last_date_gsm (product_id,etl_last_date)
        SELECT
            product_id,
            MAX(stg.date) AS max_date
        FROM product_analyzer.stg_gsm_reviews1 AS stg
        GROUP BY stg.product_id
        ON CONFLICT (product_id)
        DO UPDATE SET
            etl_last_date = EXCLUDED.etl_last_date

        """
    execute_query(db_session, query)

def update_last_date_reddit(schema_name,db_session):
    query=f"""
        INSERT INTO {schema_name}.etl_last_date_reddit (product_id,etl_last_date)
        SELECT
            product_id,
            MAX(stg.date) AS max_date
        FROM product_analyzer.stg_reddit_reviews1 AS stg
        GROUP BY stg.product_id
        ON CONFLICT (product_id)
        DO UPDATE SET
            etl_last_date = EXCLUDED.etl_last_date

        """
    execute_query(db_session, query)



def insert_into_stg(db_session,driver,url,reddit,schema_name):
    specs_df=misc_handler.return_specs_df(url,driver) 
    reviews_gsm_df=misc_handler.extract_all_reviews(url,driver)
    reviews_gsm_df=cleaning_dfs_handler.clean_reviews_gsm(reviews_gsm_df)
    specs_df=cleaning_dfs_handler.clean_specs(specs_df)
    product_id=specs_df.iloc[0,0]
    last_date_gsm_df=return_max_date_gsm(product_id,schema_name,db_session)
    if len(last_date_gsm_df)>0:
        reviews_gsm_df=reviews_gsm_df.loc[(reviews_gsm_df['Date']>(last_date_gsm_df.iloc[0,0]))]
    reviews_gsm_df=misc_handler.sentiment_analysis_df(reviews_gsm_df)
    all_reviews_reddit=misc_handler.return_all_reddit_df(specs_df,reddit)
    all_reviews_reddit=cleaning_dfs_handler.clean_reviews_reddit(all_reviews_reddit)
    last_date_reddit_df=return_max_date_reddit(product_id,schema_name,db_session)
    if len(last_date_reddit_df)>0:
        all_reviews_reddit=all_reviews_reddit.loc[(all_reviews_reddit['Date']>(last_date_reddit_df.iloc[0,0]))]
    all_reviews_reddit=misc_handler.sentiment_analysis_df(all_reviews_reddit)
    prices_df=misc_handler.return_prices_df(url,driver)
    prices_df=cleaning_dfs_handler.clean_prices(prices_df)
    prices_df=misc_handler.convert_currency_df(prices_df)
    insert_stmt_specs=return_insert_into_sql_statement_from_df(specs_df,'stg_products_specs1')
    insert_stmt_reviews=return_insert_into_sql_statement_from_df(reviews_gsm_df,'stg_gsm_reviews1')
    insert_stmt_reviews_reddit=return_insert_into_sql_statement_from_df(all_reviews_reddit,'stg_reddit_reviews1')
    insert_stmt_prices=return_insert_into_sql_statement_from_df(prices_df,'stg_products_prices1')
    for insert in insert_stmt_specs:
        execute_query(db_session=db_session,query=insert)
    for insert in insert_stmt_reviews:
        execute_query(db_session=db_session,query=insert)
    for insert in insert_stmt_reviews_reddit:
        execute_query(db_session=db_session,query=insert)
    for insert in insert_stmt_prices:
        execute_query(db_session=db_session,query=insert)


def insert_sales_stg(db_session,driver):
    try:
        sales_df=misc_handler.return_sales_per_year(driver)
        sales_df=cleaning_dfs_handler.clean_sales(sales_df)
        dst_table = f"stg_sales"
        insert_stmt = return_insert_into_sql_statement_from_df(sales_df, dst_table)
        for insert in insert_stmt:
            execute_query(db_session=db_session,query=insert)
    except Exception as error:
        print(error)



def execute_hook(input_text,sql_command_directory_path = './SQL_Commands'):
    reddit=praw.Reddit(
            client_id="A99udy2Ex7RaoBzW5O3Gdw",
            client_secret="jOKXzOzOe9sk-wn-i5a7c4I4zdac4w",
            user_agent="my-tech"
        )
    driver = webdriver.Chrome()
    # options=Options()
    # options.add_argument('--headless')
    # driver = webdriver.Chrome(options=options)
    db_session = create_connection()
    create_etl_last_date_gsm(DESTINATION_SCHEMA.DESTINATION_NAME.value,db_session)
    create_etl_last_date_reddit(DESTINATION_SCHEMA.DESTINATION_NAME.value,db_session)
    url=misc_handler.return_url_gsm_search(input_text,driver)
    insert_into_stg(db_session,driver,url,reddit,DESTINATION_SCHEMA.DESTINATION_NAME.value)
    driver.quit()
    execute_hook_sql(db_session, sql_command_directory_path)
    update_last_date_gsm(DESTINATION_SCHEMA.DESTINATION_NAME.value,db_session)
    update_last_date_reddit(DESTINATION_SCHEMA.DESTINATION_NAME.value,db_session)

    close_connection(db_session)
    