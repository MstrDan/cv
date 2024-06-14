from datetime import datetime
from logging import getLogger
import csv
import os
import numpy as np
import pandas as pd
import re as re
import subprocess as sb
import sys 
import cx_Oracle
import platform
import shutil
from datetime import date
import time
import requests
from requests.exceptions import HTTPError
from pandas import json_normalize
import zipfile
import io


#This Python script automates the population of a risk table based on business rules provided.
#Previous process involved manual work to run sql scripts, export to excel, update and import back.
# THIS SCRIPT IS FOR DEMO ONLY, IT DOES NOT WORK AS ESSENTIAL PARAMETERS, CONFIG files, folders are made inaccessible.
# Entire script implemented by Dagnaw Amare, 2022-2024. 



logger = getLogger('return')


def update_risk_factors_fy(ra_table,cursor,data_file_fields,LOG_FILE):


    #Collect existing risk factors
    
    fiscal_year = date.today().year+1 if date.today().month>9 else date.today().year

    #Check if already loaded and exit if true
    sql = """
            select * from """+ra_table+""" where monitoring_fiscal_year = """ +  str(fiscal_year+1) + """ order by risk_factor_id"""

    cursor.execute(sql).fetchall()
    if (cursor.rowcount > 0):
        print_txt = f'Risk Factor rows for {fiscal_year+1} has already been loaded'
        print(print_txt) 
        with open(LOG_FILE, 'a') as f:
            f.write(print_txt)
            f.write('\n')
        exit(0)


    #Continue load for new fiscal year
    sql = """
            select * from """+ra_table+""" where monitoring_fiscal_year = """ +  str(fiscal_year) + """ order by risk_factor_id"""

       #cngrants_duns = pd.read_sql(sql, con=connection)   
    cursor.execute(sql)
    #cngrants = pd.DataFrame(cursor.fetchall(),columns=['UEI_NBR','ORG_NAME'])
    risk_factors_df = pd.DataFrame(cursor.fetchall(),columns=data_file_fields)
    #cngrants_uei = cngrants[cngrants['UEI_NBR'].notnull()]
    num_risk_factors_rows = cursor.rowcount    
    print_txt = f'Number of risk factors for fiscal year {fiscal_year} is : %s'%cursor.rowcount
    print(print_txt) 
    with open(LOG_FILE, 'a') as f:
        f.write(print_txt)
        f.write('\n')
    #cngrants_duns.columns = [x[0] for x in cursor.description]
    #print (risk_factors_df['RISK_FACTOR_ID'].head())
    #Get latest risk factor id
    max_risk_factor_id = risk_factors_df['RISK_FACTOR_ID'].iloc[-1]
    #print('Max risk factor id is %s'%max_risk_factor_id)
    risk_row = 0
    
    #print(range(len(cngrants_uei['UEI_NBR'])))
    for risk_index in range(max_risk_factor_id+1,max_risk_factor_id+num_risk_factors_rows+1):

        risk_factors_df['RISK_FACTOR_ID'][risk_row] = risk_index
        risk_factors_df['MONITORING_FISCAL_YEAR'][risk_row] = fiscal_year+1
        #risk_factors_df['THRESH_AWARD_VAL'][risk_row] = fiscal_year+1
        risk_factors_df['CREATION_DT'][risk_row] = date.today()
        risk_factors_df['MOD_DT'][risk_row] = date.today()
        risk_row +=1

    #print(f'Data type of creation date is {type(risk_factors_df["CREATION_DT"][0])}')
    data_file_data = risk_factors_df
    WORKING_DIRECTORY = os.getcwd()
    DATA_DIR = os.path.join(WORKING_DIRECTORY, 'Data') 
    today = datetime.now().strftime("%m_%d_%Y_%H_%M")
    DATA_FILE_NAME = 'RISK_FACTORS_FY_'+today+'.csv'
    DATA_FILE = os.path.join(DATA_DIR, DATA_FILE_NAME)
    data_file_data.to_csv(DATA_FILE,index=False)
    #print(f'Data to be loaded is: {data_file_data["CRITERIA_COMMENTS_VAL"].head()}')
    print_txt = f'Risk Factors for fiscal year {fiscal_year+1} to be loaded: {len(data_file_data.index)}'        
    print(print_txt)
    with open(LOG_FILE, 'a') as f:
        f.write(print_txt)
        f.write('\n')
    #exit(0)    
    return data_file_data


def connect_oracle(v_db_env, LOG_FILE):
    #print(data_file_data.head())
    #data_file_data.to_csv(OUTPUT_FILE, index=False)
     #print("ARCH:", platform.architecture())
    lib_dir = r"C:\oracle\Product\instantclient_21_3"
    #Turn off warning SettingWithCopyWarning
    pd.options.mode.chained_assignment = None
    
    try:
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
    except Exception as err:
        print("Error connecting: cx_Oracle.init_oracle_client()")
        print(err);
        sys.exit(1);


    CURRENT_DIRECTORY = os.getcwd()    
    #ctx_auth = AuthenticationContext(SITE_URL)
    PSWD_FILE = os.path.join(CURRENT_DIRECTORY, 'Config','pswd.txt') 
    if not os.path.exists(PSWD_FILE):
        print_txt='Error: Credentials file was not found: %s '%(PSWD_FILE)
        print(print_txt)
        with open(LOG_FILE, 'a') as f:
            f.write(print_txt)
            f.write('\n')
        sys.exit(1)
    with open(PSWD_FILE) as file:
        lines = file.readlines()
    for line in lines:
        env=line.rstrip().split('=')[0]
        if (env.upper() == v_db_env.upper()):
            user_id=line.rstrip().split('=')[1].split(',')[0]
            passwd=line.rstrip().split('=')[1].split(',')[1]        
    #print('User name is: %s, password is: %s'%(user_id, passwd))


    #user_passwd = v_credentials.split('/')
    #user_id = user_passwd[0]
    #passwd = user_passwd[1]
    #print('User: '+ user + ' passwd: '+ passwd)
    #return
    #print(cx_Oracle.clientversion())
    db_dsn = 'Conn_dsn'
    try:
        connection = cx_Oracle.connect(user=user_id, password=passwd,
                               dsn=db_dsn)
    except Exception as err:
        print_txt='Error connecting: Invalid User/Password. Please check and try again.'
        print(print_txt)
        with open(LOG_FILE, 'a') as f:
            f.write(print_txt)
            f.write('\n')
        sys.exit(1)

    cursor = connection.cursor()    
    return (connection, cursor)
    
    
def load_data(connection, cursor, ra_table,data_name_curr, data_file_headers, data_file_data, LOG_FILE):
    
    start = time.time()
    print_txt='Data import start time: %s'%datetime.now().strftime("%m_%d_%Y %H:%M:%S")
    with open(LOG_FILE, 'a') as f:
            f.write(print_txt)
            f.write('\n')
    #Limit USA Spending to only CNCS Organizations
    #print(data_file_data.columns)
    #print(len(data_file_data.index))
    col_name = data_file_headers
    col_name_str = """ ("""+','.join(col_name)+""") """
    #vals = ['23456','Test Organization Name']
    #f = lambda x: "N/A" if pd.isna(x) else ("" if pd.isnull(x) else str(x))
    g = lambda x: "" if pd.isnull(x) else str(x)
    vals_vars = """ (:"""+',:'.join(col_name)+""")"""
    #sql = """ insert into """+ra_table+""" (application_id) values( :did)"""
    sql = """ insert into """+ra_table+ col_name_str + """values""" + vals_vars   
    #print(f'Insert sql is: {sql}')
    rowcount = 0
    file_recs = len(data_file_data.index)
    increment = 100 if int(file_recs/10) < 100 else int(file_recs/10) 
    increment = int(increment/100)*100
    for index, rows in data_file_data.iterrows():       
        #vals = [g(x) for x in rows]                
        vals = [x for x in rows]                
        try:
            cursor.execute(sql,vals)
        except Exception as e:
            error, = e.args
            if error.code in (12899,1438):
                print_txt = error.message
                print(print_txt)
                with open(LOG_FILE, 'a') as f:
                    f.write(print_txt)
                    f.write('\n')

                continue
            else:
                print_txt = error.message
                print(print_txt)
                with open(LOG_FILE, 'a') as f:
                    f.write(print_txt)
                    f.write('\n')
                exit(1)
        rowcount += 1
        if (rowcount%increment == 0):
            print('Inserted records %s/%s ...'%(rowcount,file_recs))
    connection.commit()
    print_txt='Successfully imported records into %s: %s/%s'%(ra_table,rowcount,file_recs)
    print(print_txt)
    #text_file = open(LOG_FILE, "wt")
    #n = text_file.write('Successfully imported %s/%s records into %s table.'%(rowcount,file_recs,ra_table))
    #text_file.close()   
    with open(LOG_FILE, 'a') as f:
        f.write(print_txt)
        f.write('\n')
        
    runtime = (time.time() - start)/60
    print_txt='Data import completion: %s'%datetime.now().strftime("%m_%d_%Y %H:%M:%S")
    print_runtime='Import duration: %d mins'%runtime
    with open(LOG_FILE, 'a') as f:
            f.write(print_txt)
            f.write('\n')
            f.write(print_runtime)
            f.write('\n')
        

def main( v_db_env):
#def main():
    #examp

    DEMO_MAX = 20000
    num_rows = 0

    WORKING_DIRECTORY = os.getcwd()
    #print('Imported Index directory is '+ INDEX_DIRECTORY)
    #print('Imported CACHE directory is '+ IRSX_CACHE_DIRECTORY)
    #print('Imported XML directory is '+ WORKING_DIRECTORY)
    today = datetime.now().strftime("%m_%d_%Y_%H_%M")
    
    #ra_table = 'DAMARE.'+tables_map[data_name_curr] 
    
    #data_file_curr = v_data_file
    data_file_log =  'Risk_factors_update_fy_log_'+today+'.txt'
    CONFIG_FILE = os.path.join(WORKING_DIRECTORY, 'Config','Data_file_structs.xls') 
    LOG_FILE = os.path.join(WORKING_DIRECTORY, 'Logs',data_file_log) 
    if not os.path.exists(CONFIG_FILE):
        print_txt='Error: Config file was not found: %s '%(CONFIG_FILE)
        print(print_txt)
        with open(LOG_FILE, 'a') as f:
            f.write(print_txt)
            f.write('\n')
        sys.exit(1)
    else:
        print_txt = "Config file: %s" %(CONFIG_FILE) 
        print(print_txt)
        with open(LOG_FILE, 'a') as f:
            f.write(print_txt)
            f.write('\n')
    
    #exit(0)
    connection, cursor = connect_oracle(v_db_env, LOG_FILE)
    
    data_name_curr = 'RISK_FACTORS'
    data_file_headers = pd.read_excel(CONFIG_FILE, names=['FIELDS','COLUMNS'],sheet_name = data_name_curr,header=None, index_col=False)
    data_file_fields = data_file_headers['FIELDS'].T.tolist()
    #data_file_cols = data_file_headers['COLUMNS'].T.tolist()
    ra_table = 'ARES.RA_DAILY_RISK_FACTORS'    
    #data_file_data = get_usaspending_data(data_file_fields,data_file_cols,LOG_FILE)        
    data_file_data = update_risk_factors_fy(ra_table,cursor,data_file_fields,LOG_FILE)
        
    load_data(connection, cursor,ra_table,data_name_curr, data_file_fields, data_file_data, LOG_FILE)
 
    #print to log file
    sys.exit(0)


if __name__ == '__main__':
    
    if len(sys.argv) != 2:        
        #raise ValueError('FAILED -- Invalid job execution format.')
        print('Error: Invalid job execution format.\n'+
                    'Please execute the script in this format. \n'+
                            'Python Risk_factors_update_fy.py [Environment].\n'+
                            'Example: Python Risk_factors_update_fy.py Test')
        sys.exit(1)
    v_db_env = sys.argv[1]
    #v_credentials = sys.argv[2]
    #v_data_name = sys.argv[2]
    
    #v_data_file = sys.argv[4]
    if (v_db_env.lower() not in ['test','prod']):
        print('Error: Invalid environment.\n'+
                    'Please use either Test/Prod for environment.\n'+ 
                            'Python Risk_factors_update_fy.py [Environment].\n'+
                            'Example: Python Risk_factors_update_fy.py Test')
        sys.exit(1)
        

    main(v_db_env)