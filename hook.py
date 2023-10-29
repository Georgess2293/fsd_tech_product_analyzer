from database_handler import execute_query, create_connection, close_connection,return_data_as_df, return_insert_into_sql_statement_from_df
from lookups import InputTypes,ETLStep,DESTINATION_SCHEMA,sql_files
from datetime import datetime
import logging
from datetime import datetime
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
    


def execute_hook_sql(db_session,sql_command_directory_path):
    for sql_file in sql_files.Hook.value:
            with open(os.path.join(sql_command_directory_path,sql_file), 'r') as file:
                sql_query = file.read()
                sql_query = sql_query.replace('target_schema', DESTINATION_SCHEMA.DESTINATION_NAME.value)
                execute_query(db_session, sql_query)



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
        reviews_gsm_df=reviews_gsm_df.loc[(reviews_gsm_df['Date']>pd.to_datetime(last_date_gsm_df.iloc[0,0]))]
    reviews_gsm_df=misc_handler.sentiment_analysis_df(reviews_gsm_df)
    all_reviews_reddit=misc_handler.return_all_reddit_df(specs_df,reddit)
    all_reviews_reddit=cleaning_dfs_handler.clean_reviews_reddit(all_reviews_reddit)
    last_date_reddit_df=return_max_date_reddit(product_id,schema_name,db_session)
    if len(last_date_reddit_df)>0:
        all_reviews_reddit=all_reviews_reddit.loc[(all_reviews_reddit['Date']>pd.to_datetime(last_date_reddit_df.iloc[0,0]))]
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


def execute_hook(input_text,reddit,sql_command_directory_path = './SQL_Commands'):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger=logging.getLogger(__name__)
    driver = webdriver.Chrome()
    # options=Options()
    # options.add_argument('--headless')
    # driver = webdriver.Chrome(options=options)
    db_session = create_connection()
    logger.info("Creating etl_last_date_gsm")
    create_etl_last_date_gsm(DESTINATION_SCHEMA.DESTINATION_NAME.value,db_session)
    logger.info("Creating etl_last_date_reddit")
    create_etl_last_date_reddit(DESTINATION_SCHEMA.DESTINATION_NAME.value,db_session)
    logger.info("Returning product URL")
    url=misc_handler.return_url_gsm_search(input_text,driver)
    logger.info("Inserting staging tables")
    insert_into_stg(db_session,driver,url,reddit,DESTINATION_SCHEMA.DESTINATION_NAME.value)
    driver.quit()
    logger.info("Executing SQL commands")
    execute_hook_sql(db_session, sql_command_directory_path)
    logger.info("Updating last date review")
    update_last_date_gsm(DESTINATION_SCHEMA.DESTINATION_NAME.value,db_session)
    update_last_date_reddit(DESTINATION_SCHEMA.DESTINATION_NAME.value,db_session)
    close_connection(db_session)
    logger.info("Hook Success")
    
    