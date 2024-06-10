import ncl_sqlsnippets as snips
import pandas as pd
import toml
from dotenv import load_dotenv

from utils.global_params import *
from utils.gpw_main import *
from utils.network_management import *
from utils.sandpit_management import *

#Load config file
config = toml.load("./config.toml")
#Load env file
load_dotenv(override=True)
env_debug = import_settings(config, "debug")

#GPW Main Pipeline
if env_debug["DEBUG_GPW_MAIN"]:
    print("#########   Processing GPW Main Pipeline   #########")

    #Load pipeline env
    env_gpw_main = import_settings(config, "gpw_main")

    #Get data files:
    ndfs = fetch_new_files(env_gpw_main["DATA_DIRECTORY"], ext=".csv")

    #For each new file, execute the pipeline
    for ndf in ndfs:

        #Fetch the data file
        file_path = env_gpw_main["DATA_DIRECTORY"] + ndf
        gpw_data = pd.read_csv(file_path)

        #Run main pipeline processing function
        df_gpw_main = process_gpw_main(gpw_data, env_gpw_main)

        #If enabled, upload the output data
        if env_debug["DEBUG_UPLOAD"]:
            #Upload resulting dataframe
            upload_pipeline_data(env_gpw_main, df_gpw_main)