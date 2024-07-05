import pandas as pd
import ncl_sqlsnippets as snips
import numpy as np

from datetime import datetime
from dateutil.relativedelta import relativedelta

def nwrs_apply_rag(data, date_data):
    
    #Determine rag 
    #Red (0):   If they logged in during the previous quarter
    #Amber (1): If they modified during the previous 2 quarters
    #Green (2): Otherwise

    #Derrive dates for the previous quarter start dates
    dd_year = int(date_data[:4])
    dd_month = int(date_data[5:7])
    if dd_month <= 3:
        qs_year = dd_year - 1
        qs_month = 10
    else:
        qs_year = dd_year
        qs_month = (int((dd_month-4)/3)*3) + 1

    qs_date_1 = datetime.strptime(str(qs_year) + "-" + str(qs_month) + "-01", 
                                "%Y-%m-%d")
    qs_date_2 = qs_date_1 - relativedelta(months=3)

    #Set up conditions for Red, Amber, Green
    conditions = ([
        (data["last_logged_in"].isnull()),    #R if no logged in date in data
        (data["last_modified"].isnull()),     #R if no modified date in data
        (data["last_logged_in"] < qs_date_1), #R if not logged in previous qtr
        (data["last_modified"] < qs_date_2),  #A if not modified in prev 2 qtrs
        (True)])                              #G otherwise
    
    #Choices (0: Red, 1: Amber, 2: Green)
    choices = [0, 0, 0, 1, 2]

    data["rag"] = np.select(conditions, choices)

    return data

def process_nwrs(data, date_data, env):

    #Filter NCL
    data_ncl = data[data["Sub ICB Code"] == "93C"]

    #Determine scope of the file
    if "GP Practice Code" in data_ncl.columns:
        scope = "GP"
    else:
        scope = "PCN"

    #Trim the data to the relevant columns
    columns_source = ["PCN Code", "PCN Name", "Last Logged In", "Last Modified"]
    columns_output = {
            "PCN Code": "pcn_code",
            "PCN Name": "pcn_name",
            "Last Logged In": "last_logged_in",
            "Last Modified": "last_modified"
        }
    
    if scope == "GP":
        columns_source += ["GP Practice Code", "GP Practice Name"]
        columns_output["GP Practice Code"] = "prac_code"
        columns_output["GP Practice Name"] = "prac_name"

    data_ncl = data_ncl[columns_source]

    #Rename existing columns
    df_nwrs_formatted = data_ncl.rename(
        columns=columns_output)

    #Set the scope and scope_id
    df_nwrs_formatted["scope"] = scope
    if scope == "GP":
        df_nwrs_formatted["scope_id"] = df_nwrs_formatted["prac_code"]
    else:
        df_nwrs_formatted["scope_id"] = df_nwrs_formatted["pcn_code"]

    #Format the date columns as date types
    df_nwrs_formatted["last_logged_in"] = pd.to_datetime(
        df_nwrs_formatted["last_logged_in"], dayfirst=True)
    df_nwrs_formatted["last_modified"] = pd.to_datetime(
        df_nwrs_formatted["last_modified"], dayfirst=True)

    #Apply the rag
    df_nwrs_output = nwrs_apply_rag(df_nwrs_formatted, date_data)

    #Add the date_data and date_extract to the data
    date_extract = datetime.today().strftime("%Y-%m-%d")
    
    df_nwrs_output["date_data"] = date_data
    df_nwrs_output["date_extract"] = date_extract

    return df_nwrs_output