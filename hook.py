from database_handler import execute_query, create_connection, close_connection,return_data_as_df, return_insert_into_sql_statement_from_df
from lookups import InputTypes, IncrementalField,ETLStep
from datetime import datetime
# from prehook import return_tables_by_schema, return_lookup_items_as_dict, execute_sql_folder
import misc_handler
import cleaning_dfs_handler

    
# def create_etl_checkpoint(schema_name , db_session):
#     query = f"""
#         CREATE TABLE IF NOT EXISTS {schema_name}.etl_checkpoint
#         (
#             etl_last_run_date TIMESTAMP
#         )
#         """
#     execute_query(db_session, query)
    
# def insert_or_update_etl_checkpoint(db_session, etl_date, does_etl_time_exists):
#     if does_etl_time_exists:
#         # update with etl_date
#         pass
#     else:
#         # insert with etl_date
#         pass

# def read_source_df_insert_dest(db_session, source_name, etl_date = None):
#     try:
#         source_name = source_name.value
#         tables = misc_handler.return_tables_by_schema(source_name)
#         incremental_date_dict = None # misc_handler.return_lookup_items_as_dict(IncrementalField)

#         for table in tables:
#             staging_query = f"""
#                     SELECT * FROM {source_name}.{table} WHERE {incremental_date_dict.get(table)} >= {etl_date}
#             """ 
#             staging_df = return_data_as_df(db_session= db_session, input_type= InputTypes.SQL, file_executor= staging_query)
#             # staging_df = staging_df[staging_df['return_date'] > etl_date]
#             if table == SQLTablesToReplicate.FILM_ACTOR.value:
#                 staging_df['film_actor_id'] = str(staging_df['actor_id']) + "-" + str(staging_df['film_id'])
#             dst_table = f"stg_{source_name}_{table}"
#             insert_stmt = return_insert_into_sql_statement_from_df(staging_df, 'dw_reporting', dst_table)
#             execute_query(db_session=db_session, query= insert_stmt)
#     except Exception as error:
#         return staging_query
    
# def read_execute_sql_transformation(db_session, sql_command_directory_path, etl_step, destination_name, is_full_refresh):
#     pass
#     # if is_full_refresh:
#     #     execute_sql_folder(db_session, sql_command_directory_path, etl_step, destination_name)
#     # else:
#     #     pass

# def return_etl_last_updated_date(db_session):
#     does_etl_time_exists = False
#     query = "SELECT etl_last_run_date FROM dw_reporting.etl_checkpoint ORDER BY etl_last_run_date DESC LIMIT 1"
#     etl_df = return_data_as_df(
#         file_executor= query,
#         input_type= InputTypes.SQL,
#         db_session= db_session
#     )
#     if len(etl_df) == 0:
#         # choose oldest day possible.
#         return_date = datetime.datetime(1992,6,19)
#     else:
#         return_date = etl_df['etl_last_run_date'].iloc[0]
#         does_etl_time_exists = True
#     return return_date, does_etl_time_exists

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

def create_etl_last_date(schema_name , db_session):
    query = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.etl_last_date
        (
            product_id INT,
            etl_last_date DATE
        )
        """
    execute_query(db_session, query)

def insert_into_last_date(schema_name,db_session):
    query=f"""
        INSERT INTO {schema_name}.etl_last_date (product_id,etl_last_date)
        SELECT
            product_id,
            MAX(review_date)
        FROM product_analyzer.fact_reviews
        GROUP BY product_id
        """
    execute_query(db_session, query)
    
def return_last_date(product_id,schema_name,db_session):
    query=f"""SELECT etl_last_date FROM {schema_name}.etl_last_date WHERE product_id={product_id}"""
    return_df=return_data_as_df(query,input_type=InputTypes.SQL,db_session=db_session)
    return return_df


def insert_into_stg(db_session,driver,url,reddit,schema_name):
    specs_df=misc_handler.return_specs_df(url,driver)
    specs_df=cleaning_dfs_handler.clean_specs(specs_df)
    product_id=specs_df.iloc[0,0]
    reviews_gsm_df=misc_handler.extract_all_reviews(url,driver)
    reviews_gsm_df=cleaning_dfs_handler.clean_reviews_gsm(reviews_gsm_df)
    last_date_df=return_last_date(product_id,schema_name,db_session)
    if len(last_date_df>0):
        reviews_gsm_df=reviews_gsm_df.loc((reviews_gsm_df['review_date']>last_date_df.iloc(0,0)))
    reviews_gsm_df=misc_handler.sentiment_analysis_df(reviews_gsm_df)
    all_reviews_reddit=misc_handler.return_all_reddit_df(specs_df,reddit)
    all_reviews_reddit=cleaning_dfs_handler.clean_reviews_reddit(all_reviews_reddit)
    if len(last_date_df>0):
        all_reviews_reddit=all_reviews_reddit.loc((all_reviews_reddit['review_date']>last_date_df.iloc(0,0)))
    all_reviews_reddit=misc_handler.sentiment_analysis_df(all_reviews_reddit)
    prices_df=misc_handler.return_prices_df(url,driver)
    prices_df=cleaning_dfs_handler.clean_prices(prices_df)
    insert_stmt_specs=return_insert_into_sql_statement_from_df(specs_df,'stg_products_specs')
    insert_stmt_reviews=return_insert_into_sql_statement_from_df(reviews_gsm_df,'stg_gsm_reviews')
    insert_stmt_reviews_reddit=return_insert_into_sql_statement_from_df(all_reviews_reddit,'stg_reddit_reviews')
    insert_stmt_prices=return_insert_into_sql_statement_from_df(specs_df,'stg_products_prices')
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

    



def execute_hook(is_full_refresh):
    print("hook")
    