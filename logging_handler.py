import logging

def show_error_message(error_string_prefix,error_string_suffix ):
    error_message = error_string_prefix + "=" + error_string_suffix
    print(error_message)

def setup_info_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(__name__)