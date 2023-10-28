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

class product_url(Enum):
    # samsung_galaxy_s20='https://www.gsmarena.com/samsung_galaxy_s20-10081.php'
    # samsung_galaxy_s20_fe_5g='https://www.gsmarena.com/samsung_galaxy_s20_fe_5g-10377.php'
    # samsung_galaxy_s20_ultra_5g='https://www.gsmarena.com/samsung_galaxy_s20_ultra_5g-10040.php'
    # samsung_galaxy_s20plus='https://www.gsmarena.com/samsung_galaxy_s20+-10080.php'
    # samsung_galaxy_s21_ultra_5g='https://www.gsmarena.com/samsung_galaxy_s21_ultra_5g-10596.php'
    # samsung_galaxy_s21plus_5g='https://www.gsmarena.com/samsung_galaxy_s21+_5g-10625.php'
    # samsung_galaxy_s22plus_5g='https://www.gsmarena.com/samsung_galaxy_s22+_5g-11252.php'
    # samsung_galaxy_a11='https://www.gsmarena.com/samsung_galaxy_a11-10132.php'
    # samsung_galaxy_a21='https://www.gsmarena.com/samsung_galaxy_a21-10172.php'
    # samasung_galaxy_a31='https://www.gsmarena.com/samsung_galaxy_a31-10149.php'
    # samsung_galaxy_a51='https://www.gsmarena.com/samsung_galaxy_a51-9963.php'
    # samsung_galaxy_a71='https://www.gsmarena.com/samsung_galaxy_a71-9995.php'
    # samsung_galaxy_a12='https://www.gsmarena.com/samsung_galaxy_a12-10604.php'
    # google_pixel_8_pro='https://www.gsmarena.com/google_pixel_8_pro-12545.php'
    # google_pixel_8='https://www.gsmarena.com/google_pixel_8-12546.php'
    # google_pixel_7='https://www.gsmarena.com/google_pixel_7-11903.php'
    # google_pixel_7_pro='https://www.gsmarena.com/google_pixel_7_pro-11908.php'
    # google_pixel_6='https://www.gsmarena.com/google_pixel_6-11037.php'
    # google_pixel_6_pro='https://www.gsmarena.com/google_pixel_6_pro-10918.php'
    # google_pixel_5='https://www.gsmarena.com/google_pixel_5-10386.php'
    # google_pixel_4='https://www.gsmarena.com/google_pixel_4-9896.php'
    # google_pixel_4_xl='https://www.gsmarena.com/google_pixel_4_xl-9895.php'
    # iphone_14='https://www.gsmarena.com/apple_iphone_14-11861.php'
    # iphone_14_pro='https://www.gsmarena.com/apple_iphone_14_pro-11860.php'
    # iphone_14_plus='https://www.gsmarena.com/apple_iphone_14_plus-11862.php'
    # iphone_14_pro_max='https://www.gsmarena.com/apple_iphone_14_pro_max-11773.php'
    # iphone_13_pro='https://www.gsmarena.com/apple_iphone_13_pro-11102.php'
    # iphone_13_pro_max='https://www.gsmarena.com/apple_iphone_13_pro_max-11089.php'
    # iphone_12_pro='https://www.gsmarena.com/apple_iphone_12_pro-10508.php'
    # iphone_12_pro_max='https://www.gsmarena.com/apple_iphone_12_pro_max-10237.php'
    # iphone_11_pro='https://www.gsmarena.com/apple_iphone_11_pro-9847.php'
    # iphone_11_pro_max='https://www.gsmarena.com/apple_iphone_11_pro_max-9846.php'
    samsung_galaxy_s21_fe_5g='https://www.gsmarena.com/samsung_galaxy_s21_fe_5g-10954.php'
    samsung_galaxy_s23_fe='https://www.gsmarena.com/samsung_galaxy_s23_fe-12520.php'
    samsung_galaxy_s23_ultra='https://www.gsmarena.com/samsung_galaxy_s23_ultra-12024.php'
    samsung_galaxy_s23='https://www.gsmarena.com/samsung_galaxy_s23-12082.php'
    samsung_galaxy_s22_5g='https://www.gsmarena.com/samsung_galaxy_s22_5g-11253.php'
    samsung_galaxy_s22_ultra_5g='https://www.gsmarena.com/samsung_galaxy_s22_ultra_5g-11251.php'
    apple_iphone_11='https://www.gsmarena.com/apple_iphone_11-9848.php'
    apple_iphone_12='https://www.gsmarena.com/apple_iphone_12-10509.php'
    apple_iphone_13='https://www.gsmarena.com/apple_iphone_13-11103.php'
    apple_iphone_15_pro_max='https://www.gsmarena.com/apple_iphone_15_pro_max-12548.php'

class sales_url(Enum):
    url='https://www.sellcell.com/how-many-mobile-phones-are-sold-each-year/#sources-and-media-contacts'

class staging_tables(Enum):
    Products_Specs='stg_products_specs1'
    Products_Prices='stg_products_prices1'
    GSM_Reviews='stg_gsm_reviews1'
    Reddit_Reviews='stg_reddit_reviews1'

class sql_files(Enum):
    Prehook=['V1_prehook_create-schema.sql']
    Hook=['V2_hook_dim-product.sql','V3_hook_fact-reviews.sql','V4_hook_dim-specs.sql','V5_hook_dim-prices.sql','V6_hook_agg-prices.sql',
          'V7_hook_agg-reviews.sql','V8_hook_vw-avg-prices.sql','V9_hook_vw-reviews-stats.sql','V10_hook_fact-sales.sql','V11_hook_vw-specs-prices.sql',
          'V12_hook_vw-reviews-sales.sql']






