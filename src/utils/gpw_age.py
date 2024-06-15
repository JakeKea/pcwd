import pandas as pd
import ncl_sqlsnippets as snips

from datetime import datetime

#Trim data to NCL only
def filter_ncl(data):
    return data.copy().loc[data["ICB_CODE"] == "QMJ"]

#Function that trims the list of all columns to just the staff roles
def read_age_columns(all_columns):

    #Must start with "TOTAL"
    candidate_columns = (
        [entry for entry in all_columns if entry.startswith("TOTAL_")])

    #Age columns contain FTE or HC in the middle
    candidate_columns = (
        [entry for entry in candidate_columns
         if "_FTE_" in entry or "_HC_" in entry]
    )

    #Remove Ethnicity (COQ) Columns
    candidate_columns = (
        [entry for entry in candidate_columns if "_COQ_" not in entry])
    
    #Remove unused GP columns
    candidate_columns = (
        [entry for entry in candidate_columns
         if "_GP_EXTG_" in entry or "_GP_" not in entry]
    )

    return candidate_columns

#Process the main data from the gpw file
def process_gpw_age(data, date_data, env):
    
    #Remap column names for old/new files
    #Not currently implemented

    #Filter to NCL only
    df_gpw_ncl = filter_ncl(data)

    #Identify relevant columns
    gpw_context_cols = ["PRAC_CODE", "PRAC_NAME"]
    gpw_sr_cols = read_age_columns(df_gpw_ncl.columns.tolist())

    #Format data for output
    ##Filter to just the context and sr columns
    df_gpw_output = df_gpw_ncl.loc[:, gpw_context_cols + gpw_sr_cols]

    df_gpw_output = pd.melt(df_gpw_output, id_vars=gpw_context_cols, var_name="source_name", value_name="staff_in_post")

    #Rename columns and add date column
    df_gpw_output = df_gpw_output.rename(columns={"PRAC_CODE": "prac_code", "PRAC_NAME": "prac_name"})
    df_gpw_output["date_data"] = date_data
    df_gpw_output["date_extract"] = datetime.today().strftime("%Y-%m-%d")

    #Remove rows with NULL SIP
    df_gpw_output = df_gpw_output.dropna(subset=['staff_in_post'])
    #Remove rows with 0 SIP
    df_gpw_output = df_gpw_output[df_gpw_output['staff_in_post'] != 0]

    #Return data
    return df_gpw_output
