import pandas as pd
import ncl_sqlsnippets as snips

from datetime import datetime

#Trim data to NCL only
def filter_ncl(data):
    return data.copy().loc[data["ICB_CODE"] == "QMJ"]

#Process the main data from the gpw file
def process_pcn(data, date_data, env):
    
    #Remap column names for old/new files
    #Not currently implemented

    #Filter to NCL only
    df_pcn_ncl = filter_ncl(data)

    #Identify relevant columns
    pcn_cols = (
        ["PCN_CODE", "PCN_NAME", 
         "STAFF_GROUP", "STAFF_ROLE", "DETAILED_STAFF_ROLE", "FTE"])

    #Format data for output
    ##Filter to just the context and sr columns
    df_pcn_agg = df_pcn_ncl.loc[:, pcn_cols]
    df_pcn_agg = df_pcn_agg.groupby(pcn_cols[:-1]).agg({"FTE":"sum"})

    #Rename columns and add date column
    df_pcn_output = df_pcn_agg.rename(
        columns={"PCN_CODE": "pcn_code", 
                 "PCN_NAME": "pcn_name",
                 "STAFF_GROUP": "staff_group",
                 "STAFF_ROLE": "staff_role",
                 "DETAILED_STAFF_ROLE": "staff_role_detailed",
                 "FTE": "fte"})
    
    

    #Add date columns
    df_pcn_output["date_data"] = date_data
    df_pcn_output["date_extract"] = datetime.today().strftime("%Y-%m-%d")

    df_pcn_output.to_csv("test_output.csv",mode="w")


    #Return data
    return df_pcn_output
