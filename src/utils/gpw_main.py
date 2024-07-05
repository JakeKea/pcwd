import pandas as pd
import ncl_sqlsnippets as snips

from datetime import datetime

#Trim data to NCL only
def filter_ncl(data):
    return data.copy().loc[data["ICB_CODE"] == "QMJ"]

#Function that trims the list of all columns to just the staff roles
def read_staff_roles(all_columns):

    #Must start with "TOTAL"
    candidate_columns = (
        [entry for entry in all_columns if entry.startswith("TOTAL_")])

    #Remove Patient data
    patient_total_columns = (["TOTAL_MALE", "TOTAL_FEMALE", 
                              "TOTAL_PATIENTS", "TOTAL_UNKNOWN_GENDER"])
    
    for pc in patient_total_columns:
        candidate_columns.remove(pc)

    #Remove Ethnicity (COQ) Columns
    candidate_columns = (
        [entry for entry in candidate_columns if "_COQ_" not in entry])
    
    #Remove Age Columns
    candidate_columns = (
        [entry for entry in candidate_columns
         if "_FTE_" not in entry and "_HC_" not in entry]
    )

    #Remove Staff Group Total Columns
    sg_cols_prefix = ["TOTAL_"]
    sg_cols_body = ["GP", "GP_EXTG", "GP_EXL", "GP_EXTGL", "NURSES", "DPC", "ADMIN"]
    sg_cols_suffix = ["_HC", "_FTE"]

    sg_cols = []
    for p in sg_cols_prefix:
        for b in sg_cols_body:
            for s in sg_cols_suffix:
                sg_cols.append(p + b + s)

    for sg in sg_cols:
        candidate_columns.remove(sg)

    return candidate_columns

#Get a list of staff roles that are accounted for in the SQL column map
def get_existing_sr_cols(env, ds):

    engine = snips.connect(env["SQL_ADDRESS"], env["SQL_DATABASE"])

    columnmap_table = (f"[{env["SQL_DATABASE"]}]." +
                       f"[{env["SQL_SCHEMA"]}].[{env["SQL_TABLE_COLUMNMAP"]}]")
    query = ("SELECT source_name " +
             f"FROM {columnmap_table} " +
             f"WHERE [ds] = '{ds}' AND [previously_flagged] IS NULL")

    return snips.execute_sfw(engine, query)

#Flag staff role columns in the column map table to acknowledged they have been
#previously flagged
def flag_deprecated_column(sr, env, ds):

    #Update SQL table
    today = datetime.today().strftime("%Y-%m-%d")

    engine = snips.connect(env["SQL_ADDRESS"], env["SQL_DATABASE"])

    columnmap_table = (f"[{env["SQL_DATABASE"]}]." +
                       f"[{env["SQL_SCHEMA"]}].[{env["SQL_TABLE_COLUMNMAP"]}]")
    
    query = (f"UPDATE {columnmap_table} " +
             f"SET [previously_flagged] = '{today}' " 
             f"WHERE [source_name] = '{sr}'")

    snips.execute_query(engine, query)

    #Update Excel reference file
    df_column_map = pd.read_excel(
        env["EXCEL_COLUMNMAP"], sheet_name="lookup", 
        dtype={"previously_flagged":"str"})

    file_columns = df_column_map.columns

    df_column_map.loc[((df_column_map["source_name"]==sr)
                       &(df_column_map["ds"]==ds), 
                       "previously_flagged")] = today

    df_column_map = df_column_map[file_columns]

    df_column_map.to_excel(
        env["EXCEL_COLUMNMAP"], sheet_name="lookup", index=False)

#Process the main data from the gpw file
def process_gpw_main(data, date_data, env):
    
    #Remap column names for old/new files
    #Not currently implemented

    #Filter to NCL only
    df_gpw_ncl = filter_ncl(data)

    #Identify relevant columns
    gpw_context_cols = ["PRAC_CODE", "PRAC_NAME"]
    gpw_sr_cols = read_staff_roles(df_gpw_ncl.columns.tolist())

    #Assess list of staff role columns
    ##Get existing mapping
    df_columnmap = get_existing_sr_cols(env, "gpw_main")
    existing_sr_cols = df_columnmap["source_name"].tolist()

    ##Flag any roles that are in the data but not the map
    bool_first_missing = True
    for sr in gpw_sr_cols:
        if sr not in existing_sr_cols:
            if bool_first_missing:
                bool_first_missing = False
                print("\nThe following roles are not mapped to any output group:")
            print("    ", sr)

    ##Flag any roles that are in the map but not the data
    bool_first_missing = True
    for sr in existing_sr_cols:
        if sr not in gpw_sr_cols:
            if bool_first_missing:
                bool_first_missing = False
                print("\nThe following roles are not found in the new data:")
            print("    ", sr)
            #For data no longer in the data, flag it in the columnmap table
            flag_deprecated_column(sr, env, "gpw_main")

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
