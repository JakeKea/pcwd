import ncl_sqlsnippets as snips
import pyodbc
import sqlalchemy.exc

#Upload the output for a given pipeline to the table specified in the env
def upload_pipeline_data(data, env, chunks=100):

    pipeline = env["PIPELINE_NAME"]

    #Upload the data
    try:
        #Connect to the database
        engine = snips.connect(env["SQL_ADDRESS"], env["SQL_DATABASE"])
        #if (snips.table_exists(engine, env["SQL_TABLE"], env["SQL_SCHEMA"])):
            #Delete the existing data
            #snips.execute_query(engine, query_del)
        #Upload the new data
        snips.upload_to_sql(data, engine, env["SQL_TABLE"], env["SQL_SCHEMA"], 
                            replace=False, chunks=chunks)

    except pyodbc.OperationalError:
        print(f"\nConnection issue when uploading the {pipeline} pipeline.\n")
    except sqlalchemy.exc.IntegrityError:
         print(f"\nDuplicate data issue when uploading the {pipeline} pipeline.\n")
    except pyodbc.ProgrammingError as e:
        raise Exception (e)