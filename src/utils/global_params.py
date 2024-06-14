import datetime
from os import getenv
from dotenv import load_dotenv

def import_settings(config, ds):

    load_dotenv(override=True)

    if ds == "debug":
        params_debug ={
                "DEBUG_GPW_MAIN": {"True": True, "False": False}[getenv("DEBUG_GPW_MAIN")],
                "DEBUG_UPLOAD":{"True": True, "False": False}[getenv("DEBUG_UPLOAD")]
            }
        return params_debug
    
    elif ds == "gpw_main":
        params_gpw ={

                "PIPELINE_NAME": config["gpw"]["main"]["pipeline_name"],

                "SQL_ADDRESS": getenv("SQL_ADDRESS"),
                "SQL_DATABASE": config["database"]["sql_database"],
                "SQL_SCHEMA": config["database"]["sql_schema"],
                "SQL_TABLE": config["gpw"]["main"]["sql_table_main"],

                "SQL_TABLE_COLUMNMAP": config["gpw"]["main"]["sql_table_columnmap"],

                "DATA_DIRECTORY": getenv("NETWORKED_DATA_PATH_DATA") 
                    + getenv("NETWORKED_DATA_PATH__SUBDRIECTORY_GPW")
            }
        return params_gpw
    else:
        raise ValueError (f"{ds} is not supported.")