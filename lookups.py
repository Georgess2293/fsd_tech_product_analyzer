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

class IncrementalField(Enum):
    RENTAL = "rental_last_update"
    FILM = "film_last_update"
    ACTOR = "actor_last_update"

class ETLStep(Enum):
    PRE_HOOK = "prehook"
    HOOK = "hook"

class DESTINATION_SCHEMA(Enum):
    DESTINATION_NAME = "product_analyzer"

class brands_url(Enum):
   #samsung="https://www.gsmarena.com/samsung-phones-f-9-0-r1-p1.php"
   apple="https://www.gsmarena.com/apple-phones-f-48-0-r1-p1.php"
   #oneplus="https://www.gsmarena.com/oneplus-phones-f-95-0-r1-p1.php"
   #xiaomi="https://www.gsmarena.com/xiaomi-phones-f-80-0-r1-p1.php"
   #huwawei="https://www.gsmarena.com/huawei-phones-f-58-0-r1-p1.php"


class page_limit(Enum):
    samsung=2
    xiaomi=3
    huwawei=2
    apple=2
    google=1
    oneplus=2


class first_time(Enum):
    reddit_reviews='Iphone 14'
    gsm_reviews='https://www.gsmarena.com/apple_iphone_14-reviews-11861p1.php'
    specs_url='https://www.gsmarena.com/apple_iphone_14-11861.php'



