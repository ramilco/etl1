import duckdb
import os

datapath = 'D:/Downloads/DE_case_study_xeneta/de_case_study/input_files/input'
dbpath = "D:/Downloads/duckdb_cli-windows-amd64/data/mydb1.db"

conn = duckdb.connect(dbpath)
c = conn.cursor()

directory = os.fsencode(datapath)
    
for file in os.listdir(directory):
    filename = os.fsdecode(file)
    if filename.endswith(".csv") and "regions" in filename: 
        query = '''
            INSERT INTO raw.regions 
            SELECT SLUG, NAME, PARENT,
            '{source_filename}' as SRC_FILE,
            current_timestamp as RAW_LOAD_TIME,
            'NEW' as STG_STATUS,
            null as STG_LOAD_TIME,
            nextval('serial') as RAW_PID
            FROM 
            read_csv_auto('{source_filedir}');
            '''.format(source_filename=filename, source_filedir=datapath+'/'+filename) 
        c.execute(query)      
        os.replace(datapath + "/" + filename, datapath + "/loaded/" + filename)  
    
    elif filename.endswith(".csv") and "ports" in filename:
        query = '''
            INSERT INTO raw.ports 
            SELECT PID,CODE,SLUG,NAME,COUNTRY,COUNTRY_CODE,
            '{source_filename}' as SRC_FILE,
            current_timestamp as RAW_LOAD_TIME,
            'NEW' as STG_STATUS,
            null as STG_LOAD_TIME,
            nextval('serial') as RAW_PID
            FROM 
            read_csv_auto('{source_filedir}');
            '''.format(source_filename=filename, source_filedir=datapath+'/'+filename) 
        c.execute(query) 
        os.replace(datapath + "/" + filename, datapath + "/loaded/" + filename)  
    elif filename.endswith(".csv") and "exchange_rates" in filename:
        query = '''
            INSERT INTO raw.exchange_rates
            SELECT DAY,CURRENCY,RATE,
            '{source_filename}' as SRC_FILE,
            current_timestamp as RAW_LOAD_TIME,
            'NEW' as STG_STATUS,
            null as STG_LOAD_TIME,
            nextval('serial') as RAW_PID
            FROM 
            read_csv_auto('{source_filedir}');
            '''.format(source_filename=filename, source_filedir=datapath+'/'+filename) 
        c.execute(query) 
        os.replace(datapath + "/" + filename, datapath + "/loaded/" + filename)  
    elif filename.endswith(".csv") and "charges" in filename:
        query = '''
            INSERT INTO raw.charges
            SELECT D_ID,CURRENCY,CHARGE_VALUE,
            '{source_filename}' as SRC_FILE,
            current_timestamp as RAW_LOAD_TIME,
            'NEW' as STG_STATUS,
            null as STG_LOAD_TIME,
            nextval('serial') as RAW_PID
            FROM 
            read_csv_auto('{source_filedir}');
            '''.format(source_filename=filename, source_filedir=datapath+'/'+filename) 
        c.execute(query) 
        os.replace(datapath + "/" + filename, datapath + "/loaded/" + filename)   
    elif filename.endswith(".csv") and "datapoints" in filename:
        query = '''
            INSERT INTO raw.datapoints
            SELECT D_ID, CREATED, ORIGIN_PID,DESTINATION_PID,
            VALID_FROM,VALID_TO,COMPANY_ID,SUPPLIER_ID,EQUIPMENT_ID,
            '{source_filename}' as SRC_FILE,
            current_timestamp as RAW_LOAD_TIME,
            'NEW' as STG_STATUS,
            null as STG_LOAD_TIME,
            nextval('serial') as RAW_PID
            FROM 
            read_csv_auto('{source_filedir}');
            '''.format(source_filename=filename, source_filedir=datapath+'/'+filename) 
        c.execute(query) 
        os.replace(datapath + "/" + filename, datapath + "/loaded/" + filename)         
    else:
        #write to log and send notification
        continue


conn.close()    