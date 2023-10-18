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

def insert_specs_gsm_reviews_stg(db_session,reddit,all_specs,all_reviews):
    # all_specs,all_reviews=misc_handler.return_stg_specs_exception_df(driver)
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
    insert_stmt_reviews=return_insert_into_sql_statement_from_df(all_reviews_reddit,'stg_reddit_reviews')
    for insert in insert_stmt_reviews:
        execute_query(db_session=db_session,query=insert)
    



def execute_hook(is_full_refresh):
    print("hook")
    # db_session = create_connection()
    # create_etl_checkpoint(db_session)
    # etl_date, does_etl_time_exists = return_etl_last_updated_date(db_session)
    # start_time = datetime.datetime.now()
    # read_source_df_insert_dest(db_session,SourceName.DVD_RENTAL, etl_date)
    # end_time = datetime.datetime.now()
    # misc_handler.insert_into_etl_logging_table(DestinationName.Datawarehouse, db_session, HookSteps.WRITE_TO_DW, start_time, end_time)

    # read_execute_sql_transformation(db_session, './SQL_Commands', ETLStep.HOOK, DestinationName.Datawarehouse, is_full_refresh)
    # # last step
    # insert_or_update_etl_checkpoint(db_session, datetime.now(), does_etl_time_exists)
    # close_connection()


# for index, row in rental_per_customer.iterrows():
#     existing_count_query = f"SELECT total_rentals FROM public.customer_rental_count WHERE customer_id = {row['customer_id']};"
#     existing_query_df = database_handler.return_data_as_df(file_executor= existing_count_query, input_type= lookups.InputTypes.SQL, db_session= db_session)
#     length_df = len(existing_query_df)
#     if length_df == 0:
#         insert_query = f"INSERT INTO public.customer_rental_count (customer_id, total_rentals) VALUES ({row['customer_id']}, {row['total_rentals']})"
#         database_handler.execute_query(db_session, insert_query)
#     else:
#         new_count = existing_query_df['total_rentals'][0] + row['total_rentals']
#         update_query = f"UPDATE public.customer_rental_count SET total_rentals = {new_count} WHERE customer_id = {row['customer_id']};"
#         database_handler.execute_query(db_session, update_query)