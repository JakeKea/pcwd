import datetime
from os import getenv
from dotenv import load_dotenv

def import_settings(config, ds):

    load_dotenv(override=True)

    if ds == "debug":
        params_debug ={
                "DEBUG_GPW_MAIN": {"True": True, "False": False}[getenv("DEBUG_GPW_MAIN")],
                "DEBUG_GPW_AGE": {"True": True, "False": False}[getenv("DEBUG_GPW_AGE")],
                "DEBUG_PCN": {"True": True, "False": False}[getenv("DEBUG_PCN")],
                "DEBUG_NWRS": {"True": True, "False": False}[getenv("DEBUG_NWRS")],
                "DEBUG_UPLOAD":{"True": True, "False": False}[getenv("DEBUG_UPLOAD")]
            }
        return params_debug
    
    elif ds == "gpw_main":
        params_gpw_main ={
            "PIPELINE_NAME": config["gpw"]["main"]["pipeline_name"],

            "SQL_ADDRESS": getenv("SQL_ADDRESS"),
            "SQL_DATABASE": config["database"]["sql_database"],
            "SQL_SCHEMA": config["database"]["sql_schema"],
            "SQL_TABLE": config["gpw"]["main"]["sql_table_main"],

            "SQL_TABLE_COLUMNMAP": config["gpw"]["main"]["sql_table_columnmap"],

            "DATA_DIRECTORY": getenv("NETWORKED_DATA_PATH_DATA") 
                + getenv("NETWORKED_DATA_PATH__SUBDRIECTORY_GPW")
        }
        return params_gpw_main
    
    elif ds == "gpw_age":
        params_gpw_age={
            "PIPELINE_NAME": config["gpw"]["age"]["pipeline_name"],

            "SQL_ADDRESS": getenv("SQL_ADDRESS"),
            "SQL_DATABASE": config["database"]["sql_database"],
            "SQL_SCHEMA": config["database"]["sql_schema"],
            "SQL_TABLE": config["gpw"]["age"]["sql_table_age"],

            "DATA_DIRECTORY": getenv("NETWORKED_DATA_PATH_DATA") 
                + getenv("NETWORKED_DATA_PATH__SUBDRIECTORY_GPW")
        }
        return params_gpw_age
    
    elif ds == "pcn":
        params_pcn={
            "PIPELINE_NAME": config["pcn"]["pipeline_name"],

            "SQL_ADDRESS": getenv("SQL_ADDRESS"),
            "SQL_DATABASE": config["database"]["sql_database"],
            "SQL_SCHEMA": config["database"]["sql_schema"],
            "SQL_TABLE": config["pcn"]["sql_table_pcn"],

            "DATA_DIRECTORY": getenv("NETWORKED_DATA_PATH_DATA") 
                + getenv("NETWORKED_DATA_PATH__SUBDRIECTORY_PCN")
        }
        return params_pcn

    elif ds == "nwrs":
        params_nwrs={
            "PIPELINE_NAME": config["nwrs"]["pipeline_name"],

            "SQL_ADDRESS": getenv("SQL_ADDRESS"),
            "SQL_DATABASE": config["database"]["sql_database"],
            "SQL_SCHEMA": config["database"]["sql_schema"],
            "SQL_TABLE": config["nwrs"]["sql_table_nwrs"],

            "DATA_DIRECTORY": getenv("NETWORKED_DATA_PATH_DATA") 
                + getenv("NETWORKED_DATA_PATH__SUBDRIECTORY_NWRS")
        }

    else:
        raise ValueError (f"{ds} is not supported.")