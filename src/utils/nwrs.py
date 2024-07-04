import pandas as pd
import ncl_sqlsnippets as snips

from datetime import datetime

def process_nwrs(data, date_data, env):

    #Filter NCL
    data_ncl = data[data["Sub ICB Code"] == "93C"]
    
    print(data_ncl.head())