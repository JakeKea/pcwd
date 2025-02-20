import calendar
import ncl_sqlsnippets as snips
import pandas as pd
import re
import toml
from datetime import datetime
from dotenv import load_dotenv

from utils.global_params import *
from utils.gpw_main import *
from utils.gpw_age import *
from utils.pcn import *
from utils.nwrs import *
from utils.network_management import *
from utils.sandpit_management import *

#Get the date from a given filename
def get_date_from_filename(ndf, pln, inc_day=False):
    
    #Look for the file date in the file name
    #This should be in the form of "{Month} {YYYY}", i.e. "March 2024"
    for month in calendar.month_name[1:]:
        if month in ndf:
            #Get the year from the file name
            month_idx_start = ndf.find(month)
            month_idx_end = month_idx_start + len(month) + 1
            year_candidate = ndf[month_idx_end: month_idx_end + 4]

            #Check the characters following the month is a year
            if bool(re.fullmatch(r'\d{4}', year_candidate)):

                #If needed, get the day (for the NWRS files)
                if inc_day:
                    #Get the day from the file name
                    day_candidate = ndf[month_idx_start-3:month_idx_start-1]

                    #Check the characters are a numeric value
                    if bool(re.fullmatch(r'\d{2}', day_candidate)):
                        #Convert to formatted date string (YYYY-MM-DD)
                        date_in_file = (
                            day_candidate + " " + month + " " + year_candidate)
                        file_date_date = datetime.strptime(
                            date_in_file, '%d %B %Y')
                        file_date_str = file_date_date.strftime("%Y-%m-%d")
                        
                        #Derrive the name for the archive file
                        if "PCN+" in ndf or "PCN " in ndf:
                            scope_data = "PCN"
                        else:
                            scope_data = "GP"
                        filename_archive = (f"{pln} {scope_data} " + 
                                            f"{file_date_str} - {date_in_file}")

                        return file_date_str, filename_archive
                    else:
                        break
                
                #Convert to formatted date string (YYYY-MM-DD)
                date_in_file = month + " " + year_candidate
                file_date_date = datetime.strptime(date_in_file, '%B %Y')
                file_date_str = file_date_date.strftime("%Y-%m-%d")

                #Derrive the name for the archive file
                filename_archive = f"{pln} {file_date_str} - {date_in_file}"

                return file_date_str, filename_archive
            
    print(f"Warning! No date found in the filename of '{ndf}'",
                "so it was not processed.",
                "\nThis code expects the date of the file in the form",
                "'{Month} {YYYY}' to appear in the filename", 
                "(i.e. 'April 2024')")

    return False

#Load config file
config = toml.load("./config.toml")
#Load env file
load_dotenv(override=True)
env_debug = import_settings(config, "debug")

#GPW Main Pipeline
if env_debug["DEBUG_GPW_MAIN"]:
    print("#########   Processing GPW Main Pipeline   #########\n")

    #Load pipeline env
    env_gpw_main = import_settings(config, "gpw_main")

    #Get data files:
    ndfs = fetch_new_files(env_gpw_main["DATA_DIRECTORY"], ext=".csv")
 
    #For each new file, execute the pipeline
    for ndf in ndfs:

        print(ndf)
        
        #Confirm the ndf has the time period in the name
        file_date, filename_archive = get_date_from_filename(
            ndf, env_gpw_main["PIPELINE_NAME"])
        
        if file_date:
            #Fetch the data file
            data_dir = env_gpw_main["DATA_DIRECTORY"]
            file_path = env_gpw_main["DATA_DIRECTORY"] + ndf
            
            #Sometimes the file has inconsistent encoding so try both
            try:
                df_gpw_data = pd.read_csv(file_path)
            except:
                df_gpw_data = pd.read_csv(file_path, encoding='ISO-8859-1')

            #Run main pipeline processing function
            df_gpw_main = process_gpw_main(df_gpw_data, file_date, env_gpw_main)

            #If enabled, upload the output data
            if env_debug["DEBUG_UPLOAD"]:
                #Upload resulting dataframe
                res = upload_pipeline_data(df_gpw_main, env_gpw_main)

                #If the upload was successful
                ## And archiving is enabled through ARCHIVE_FORCE_GPW
                ## And the gpw age pipeline isn't due to execute
                if (res 
                    and env_debug["ARCHIVE_FORCE_GPW"] 
                    and not env_debug["DEBUG_GPW_AGE"]):
                    try:
                        archive_data_file(
                            ndf, data_dir, filename_archive, ".csv")
                    except FileExistsError:
                        print(f"Unable to archive ecist file as there is",
                                f"already a file named {ndf}",
                                f"in the archive folder.")                  
        print()

#GPW Age Pipeline
if env_debug["DEBUG_GPW_AGE"]:
    print("#########   Processing GPW Age Pipeline   #########\n")

    #Load pipeline env
    env_gpw_age = import_settings(config, "gpw_age")

    #Get data files:
    ndfs = fetch_new_files(env_gpw_age["DATA_DIRECTORY"], ext=".csv")

    #For each new file, execute the pipeline
    for ndf in ndfs:

        print(ndf)

        #Confirm the ndf has the time period in the name
        file_date, filename_archive = get_date_from_filename(
            ndf, env_gpw_age["PIPELINE_NAME"])
        
        if file_date:
            #Fetch the data file
            data_dir = env_gpw_age["DATA_DIRECTORY"]
            file_path = env_gpw_age["DATA_DIRECTORY"] + ndf

            #Sometimes the file has inconsistent encoding so try both
            try:
                df_gpw_data = pd.read_csv(file_path)
            except:
                df_gpw_data = pd.read_csv(file_path, encoding='ISO-8859-1')

            #Run main pipeline processing function
            df_gpw_age = process_gpw_age(df_gpw_data, file_date, env_gpw_age)

            #If enabled, upload the output data
            if env_debug["DEBUG_UPLOAD"]:
                #Upload resulting dataframe
                res = upload_pipeline_data(df_gpw_age, env_gpw_age)

                #If the upload was successful
                ## And the archive functionality is enabled
                ### Either through ARCHIVE_SOURCE and both pipelines are enables
                ### Or through ARCHIVE_FORCE_GPW
                if (res and 
                    ((env_debug["ARCHIVE_SOURCE"] 
                    and env_debug["DEBUG_GPW_MAIN"])
                    or env_debug["ARCHIVE_FORCE_GPW"])):
                    try:
                        archive_data_file(
                            ndf, data_dir, filename_archive, ".csv")
                    except FileExistsError:
                        print(f"Unable to archive ecist file as there is",
                                f"already a file named {ndf}",
                                f"in the archive folder.")
    print()

#PCN Pipeline
if env_debug["DEBUG_PCN"]:
    print("#########   Processing PCN Pipeline   #########")

    #Load pipeline env
    env_pcn = import_settings(config, "pcn")

    #Get data files:
    ndfs = fetch_new_files(env_pcn["DATA_DIRECTORY"], ext=".csv")

    #For each new file, execute the pipeline
    for ndf in ndfs:

        print(ndf)

        #Confirm the ndf has the time period in the name
        file_date, filename_archive = get_date_from_filename(
            ndf, env_pcn["PIPELINE_NAME"])

        if file_date:
            #Fetch the data file
            data_dir = env_pcn["DATA_DIRECTORY"]
            file_path = env_pcn["DATA_DIRECTORY"] + ndf

            #Sometimes the file has inconsistent encoding so try both
            try:
                df_pcn_in = pd.read_csv(file_path)
            except:
                df_pcn_in = pd.read_csv(file_path, encoding='ISO-8859-1')

            #Run main pipeline processing function
            df_pcn_out = process_pcn(
                df_pcn_in, file_date, env_pcn)

            #If enabled, upload the output data
            if env_debug["DEBUG_UPLOAD"]:
                #Upload resulting dataframe
                res = upload_pipeline_data(df_pcn_out, env_pcn)

                #If the upload was successful
                ##And archiving is enabled
                if res and env_debug["ARCHIVE_SOURCE"]:
                    try:
                        archive_data_file(
                            ndf, data_dir, filename_archive, ".csv")
                    except FileExistsError:
                        print(f"Unable to archive ecist file as there is",
                                f"already a file named {ndf}",
                                f"in the archive folder.")
    print()
                
#NWRS Pipeline
if env_debug["DEBUG_NWRS"]:
    print("#########   Processing NWRS Metadata Pipeline   #########")

    #Load pipeline env
    env_nwrs = import_settings(config, "nwrs")

    #Get data files:
    ndfs = fetch_new_files(env_nwrs["DATA_DIRECTORY"], ext=".csv")

    #For each new file, execute the pipeline
    for ndf in ndfs:

        print(ndf)

        #Confirm the ndf has the time period in the name
        file_date, filename_archive = get_date_from_filename(
            ndf, env_nwrs["PIPELINE_NAME"], inc_day=True)
        
        if file_date:
            #Fetch the data file
            data_dir = env_nwrs["DATA_DIRECTORY"]
            file_path = data_dir + ndf

            #Sometimes the file has inconsistent encoding so try both
            try:
                df_nwrs = pd.read_csv(file_path)
            except:
                df_nwrs = pd.read_csv(file_path, encoding='ISO-8859-1')

            #Run main pipeline processing function
            df_nwrs = process_nwrs(df_nwrs, file_date, env_nwrs)

            #If enabled, upload the output data
            if env_debug["DEBUG_UPLOAD"]:
                #Upload resulting dataframe
                res = upload_pipeline_data(df_nwrs, env_nwrs)

                #If the upload was successful
                ##And archiving is enabled
                if res and env_debug["ARCHIVE_SOURCE"]:
                    try:
                        archive_data_file(
                            ndf, data_dir, filename_archive, ".csv")
                    except FileExistsError:
                        print(f"Unable to archive ecist file as there is",
                                f"already a file named {ndf}",
                                f"in the archive folder.")
    print()
