import os
from database_handler import execute_query, create_connection, close_connection,return_data_as_df, return_create_statement_from_df
from lookups import ErrorHandling,  InputTypes, ETLStep, DESTINATION_SCHEMA,first_time
from logging_handler import show_error_message
import misc_handler
from cleaning_dfs_handler import clean_reviews_gsm,clean_reviews_reddit,clean_specs,clean_prices,clean_sales
import datetime
import pandas as pd
import praw
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException,WebDriverException
from selenium.webdriver.common.action_chains import ActionChains
# append the execute sql folder
# execute_sql_prehook
# execute_sql_hook

def execute_prehook_sql(db_session, sql_command_directory_path):
    sql_files =misc_handler.retreive_sql_files(sql_command_directory_path)
    for sql_file in sql_files:
        if str(sql_file.split('-')[0].split('_')[1]) == ETLStep.PRE_HOOK.value:
            with open(os.path.join(sql_command_directory_path,sql_file), 'r') as file:
                sql_query = file.read()
                sql_query = sql_query.replace('target_schema', DESTINATION_SCHEMA.DESTINATION_NAME.value)
                execute_query(db_session, sql_query)
                
        

def create_sql_staging_table_index(db_session,source_name, table_name, index_val):
    query = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{index_val} ON {source_name}.{table_name} ({index_val});"
    execute_query(db_session,query)

def create_sql_staging_tables_reddit(db_session, driver,reddit):
    try:
        staging_reddit=misc_handler.extract_reddit_comments(first_time.reddit_reviews.value,reddit)
        staging_reddit=clean_reviews_reddit(staging_reddit)
        staging_reddit=misc_handler.sentiment_analysis_df(staging_reddit)
        staging_reddit.insert(2,'product_id',1)
        dst_table = f"stg_reddit_reviews1"
        create_stmt = return_create_statement_from_df(staging_reddit,DESTINATION_SCHEMA.DESTINATION_NAME.value, dst_table)
        execute_query(db_session=db_session, query= create_stmt)
        #create_sql_staging_table_index(db_session, 'dw_reporting', dst_table, columns[0])
    except Exception as error:
        print(error)


def create_sql_staging_tables_gsm_reviews(db_session, driver):
    try:
        staging_gsm_reviews=misc_handler.extract_reviews_from_page(first_time.gsm_reviews.value,driver)
        staging_gsm_reviews=pd.DataFrame(staging_gsm_reviews)
        staging_gsm_reviews=clean_reviews_gsm(staging_gsm_reviews)
        staging_gsm_reviews=misc_handler.sentiment_analysis_df(staging_gsm_reviews)
        dst_table = f"stg_gsm_reviews1"
        create_stmt = return_create_statement_from_df(staging_gsm_reviews,DESTINATION_SCHEMA.DESTINATION_NAME.value, dst_table)
        execute_query(db_session=db_session, query= create_stmt)
        #create_sql_staging_table_index(db_session, 'dw_reporting', dst_table, columns[0])
    except Exception as error:
        print(error)

def create_sql_staging_tables_specs(db_session, driver):
    try:
        columns=['product_id','brand','model','Network_Technology','Network_2G bands','Network_3G bands','Network_4G bands','Network_5G bands',
                'Network_Speed','Launch_Announced','Launch_Status','Body_Dimensions','Body_Weight','Body_Build','Body_SIM','Display_Type','Display_Size','Display_Resolution',
                'Display_Protection','Platform_OS','Platform_Chipset','Platform_CPU','Platform_GPU','Memory_Card slot','Memory_Internal','Main Camera_Triple',
                'Main Camera_Features','Main Camera_Video','Selfie camera_Single','Selfie camera_Features','Selfie camera_Video','Sound_Loudspeaker','Sound_3.5mm jack',
                'Comms_WLAN','Comms_Bluetooth','Comms_Positioning','Comms_NFC','Comms_Radio','Comms_USB','Features_Sensors','Battery_Type','Battery_Charging',
                'Misc_Colors','Misc_Models','Misc_SAR','Main Camera_Quad','Misc_SAR EU','Main Camera_Dual','Comms_Infrared port','Misc_Price','Selfie camera_Dual','Main Camera_Single']
        staging_specs = pd.DataFrame(columns=columns)
        #staging_specs=misc_handler.return_specs_df(first_time.specs_url.value,driver)
        staging_specs=clean_specs(staging_specs)
        dst_table = f"stg_products_specs1"
        create_stmt = return_create_statement_from_df(staging_specs,DESTINATION_SCHEMA.DESTINATION_NAME.value, dst_table)
        execute_query(db_session=db_session, query= create_stmt)
        #create_sql_staging_table_index(db_session, 'dw_reporting', dst_table, columns[0])
    except Exception as error:
        print(error)

def create_sql_staging_tables_prices(db_session, driver):
    try:  
        columns=['product_id', '_128GB_8GB_RAM', '_128GB_6GB_RAM', '_256GB_8GB_RAM',
       '_128GB_12GB_RAM', '_512GB_16GB_RAM', '_256GB_12GB_RAM', '_32GB_3GB_RAM',
       '_64GB_4GB_RAM', '_128GB_4GB_RAM', '_512GB_12GB_RAM', '_64GB_6GB_RAM',
       '_256GB_6GB_RAM', '_512GB_6GB_RAM', '_256GB_4GB_RAM', '_512GB_4GB_RAM',
       '_512GB_8GB_RAM','_1TB_16GB_RAM']
        columns_float=['_128GB_8GB_RAM', '_128GB_6GB_RAM', '_256GB_8GB_RAM',
       '_128GB_12GB_RAM', '_512GB_16GB_RAM', '_256GB_12GB_RAM', '_32GB_3GB_RAM',
       '_64GB_4GB_RAM', '_128GB_4GB_RAM', '_512GB_12GB_RAM', '_64GB_6GB_RAM',
       '_256GB_6GB_RAM', '_512GB_6GB_RAM', '_256GB_4GB_RAM', '_512GB_4GB_RAM',
       '_512GB_8GB_RAM','_1TB_16GB_RAM']
        
        staging_prices = pd.DataFrame(columns=columns)
        #staging_specs=misc_handler.return_specs_df(first_time.specs_url.value,driver)
        # staging_prices=clean_prices(staging_prices)
        staging_prices[columns_float]=staging_prices[columns_float].astype('float64')
        col=['product_id']
        staging_prices[col]=staging_prices[col].astype('int64')
        dst_table = f"stg_products_prices1"
        create_stmt = return_create_statement_from_df(staging_prices,DESTINATION_SCHEMA.DESTINATION_NAME.value, dst_table)
        execute_query(db_session=db_session, query= create_stmt)
        #create_sql_staging_table_index(db_session, 'dw_reporting', dst_table, columns[0])
    except Exception as error:
       print(error)

def create_sql_staging_tables_sales(db_session,driver):
    try:
        sales_df=misc_handler.return_sales_per_year_prehook(driver)
        sales_df=clean_sales(sales_df)
        dst_table = f"stg_sales1"
        create_stmt = return_create_statement_from_df(sales_df,DESTINATION_SCHEMA.DESTINATION_NAME.value, dst_table)
        execute_query(db_session=db_session, query= create_stmt)
    except Exception as error:
        print(error)


def execute_prehook(sql_command_directory_path = './SQL_Commands'):
    print("Prehook")
    step_name = ""
    try:
        reddit=praw.Reddit(
            client_id="A99udy2Ex7RaoBzW5O3Gdw",
            client_secret="jOKXzOzOe9sk-wn-i5a7c4I4zdac4w",
            user_agent="my-tech"
        )
        options=Options()
        options.add_argument('--headless')
        driver = webdriver.Chrome(options=options)
        db_session = create_connection()
        start_time = datetime.datetime.now()
        step_name=1
        print(datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y")," step:",step_name," executing prehook sql file")
        execute_prehook_sql(db_session, sql_command_directory_path) 
        step_name=2
        print(datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y")," step:",step_name," creating reviews staging table")
        create_sql_staging_tables_reddit(db_session, driver,reddit)
        step_name=3
        print(datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y")," step:",step_name," creating reviews 2nd staging table")
        create_sql_staging_tables_gsm_reviews(db_session, driver)
        step_name=4
        print(datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y")," step:",step_name," creating specs staging table")
        create_sql_staging_tables_specs(db_session, driver)
        step_name=5
        print("step:",step_name)
        create_sql_staging_tables_prices(db_session, driver)
        print(datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y")," step:",step_name," creating prices staging table")
        #create_sql_staging_tables_sales(db_session,driver)
        print(datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y")," step:",step_name," prehook finished")
        close_connection(db_session)
        driver.quit()
    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.PREHOOK_SQL_ERROR
        show_error_message(error_prefix.value, suffix)
        raise Exception(f"Important Step Failed step = {step_name}")