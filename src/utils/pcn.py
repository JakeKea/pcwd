import numpy as np
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
         "STAFF_GROUP", "STAFF_ROLE", "FTE"])

    #Format data for output
    ##Filter to just the context and sr columns
    df_pcn_agg = df_pcn_ncl.loc[:, pcn_cols]
    df_pcn_agg = df_pcn_agg.groupby(pcn_cols[:-1]).agg(
        FTE=("FTE", "sum"),
        HC = ("FTE", "size")
        ).reset_index()

    #Rename columns and add date column
    df_pcn_output = df_pcn_agg.rename(
        columns={"PCN_CODE": "pcn_code", 
                 "PCN_NAME": "pcn_name",
                 "STAFF_GROUP": "staff_group",
                 "STAFF_ROLE": "staff_role",
                 "FTE": "fte",
                 "HC": "hc"})
    
    #Format the data in the same format as the GPW data
    df_pcn_output_fte = df_pcn_output.copy().drop(columns=["hc"])
    df_pcn_output_fte["type"] = "FTE"
    df_pcn_output_fte = df_pcn_output_fte.rename(
        columns={"fte":"staff_in_post"})

    df_pcn_output_hc = df_pcn_output.copy().drop(columns=["fte"])
    df_pcn_output_hc["type"] = "HC"
    df_pcn_output_hc = df_pcn_output_hc.rename(
        columns={"hc":"staff_in_post"})

    df_pcn_output_both = pd.concat([df_pcn_output_fte, df_pcn_output_hc])

    #Add date columns
    df_pcn_output_both["date_data"] = date_data
    df_pcn_output_both["date_extract"] = datetime.today().strftime("%Y-%m-%d")

    #Return data
    return df_pcn_output_both
