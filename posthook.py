from database_handler import execute_query, create_connection,close_connection
import misc_handler
import lookups
import datetime
import logging

def truncate_staging_tables(schema_name, table_list, db_session):
    for table in table_list:
        dst_table = f"{table}"
        truncate_query = f"""
        TRUNCATE TABLE {schema_name}.{dst_table}"""
        execute_query(db_session, truncate_query)

def execute_posthook():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger=logging.getLogger(__name__)
    db_session = create_connection()
    tables = misc_handler.return_stg_tables_as_list()
    #print(datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y"),"executing prehook sql file")
    logger.info("Truncating staging tables")
    truncate_staging_tables(lookups.DESTINATION_SCHEMA.DESTINATION_NAME.value,tables,db_session)
    close_connection(db_session)

   