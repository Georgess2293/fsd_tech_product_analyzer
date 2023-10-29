from enum import Enum

class ErrorHandling(Enum):
    DB_CONNECT_ERROR = "DB Connect Error"
    DB_RETURN_QUERY_ERROR = "DB Return Query Error"
    API_ERROR = "Error calling API"
    RETURN_DATA_CSV_ERROR = "Error returning CSV"
    RETURN_DATA_EXCEL_ERROR = "Error returning Excel"
    RETURN_DATA_SQL_ERROR = "Error returning SQL"
    RETURN_DATA_UNDEFINED_ERROR = "Cannot find File type"
    EXECUTE_QUERY_ERROR = "Error executing the query"
    NO_ERROR = "No Errors"
    PREHOOK_SQL_ERROR = "Prehook: SQL Error"
    PREHOOK_CLOSE_CONNECTION_ERROR = "Error closing connection"
    HOOK_DICT_RETURN_ERROR = "Error returning lookup items as dict"
    DB_RETURN_INSERT_INTO_SQL_STMT_ERROR = "Return insert into sql dataframe error:"
    PANDAS_NULLS_ERROR = "Error dropping nulls from df"
    PANDAS_FILL_NULLS_ERROR="Error replacing nulls from df"
    

class InputTypes(Enum):
    SQL = "SQL"
    CSV = "CSV"
    EXCEL = "Excel"
    
class PreHookSteps(Enum):
    EXECUTE_SQL_QUERY = "execute_sql_folder"
    CREATE_SQL_STAGING = "create_sql_staging_tables"

class ETLStep(Enum):
    PRE_HOOK = "prehook"
    HOOK = "hook"

class DESTINATION_SCHEMA(Enum):
    DESTINATION_NAME = "product_analyzer"

class first_time(Enum):
    reddit_reviews='Iphone 14'
    gsm_reviews='https://www.gsmarena.com/apple_iphone_14-reviews-11861p1.php'
    specs_url='https://www.gsmarena.com/apple_iphone_14-11861.php'


class staging_tables(Enum):
    Products_Specs='stg_products_specs1'
    Products_Prices='stg_products_prices1'
    GSM_Reviews='stg_gsm_reviews1'
    Reddit_Reviews='stg_reddit_reviews1'

class sql_files(Enum):
    Prehook=['V1_prehook_create-schema.sql']
    Hook=['V2_hook_dim-product.sql','V3_hook_fact-reviews.sql','V4_hook_dim-specs.sql','V5_hook_dim-prices.sql','V6_hook_agg-prices.sql',
          'V7_hook_agg-reviews.sql','V8_hook_vw-avg-prices.sql','V9_hook_vw-reviews-stats.sql','V10_hook_vw-specs-prices.sql',
          'V11_hook_vw-reviews-sales.sql']






