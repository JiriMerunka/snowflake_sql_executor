#!/usr/bin/env python3

import re
import os
from subprocess import check_output
import json
import time
import snowflake.connector # Snowflake database adapter for Python
import boto3
import shutil
import pandas as pd
import sys

############################################################  

def read_manifest(file_name):
    f = open(file_name, "rt")
    manifest_content = f.read()
    f.close()
    config = json.loads(manifest_content)
    return config

def create_dirs(config):
   
    flow_choice = sys.argv[1] #take first argument from CLI, e.g. 'python3 snowflake_sql_executor.py core_dl' , the first_argument = core_dl

    exec_dir = config[flow_choice + "_exec_dir"]
    tmp_dir = config[flow_choice + "_tmp_dir"]

    path_exec_dir = os.getcwd() + '/' + exec_dir
    path_tmp_dir = os.getcwd() + '/' + tmp_dir


    print('\n[{}] --1-- Creation of dirs\n'.format(time.ctime()))
    try:
              
        shutil.rmtree(path_exec_dir, ignore_errors=True)
        shutil.rmtree(path_tmp_dir, ignore_errors=True)

        os.makedirs( path_exec_dir, exist_ok=True)
        print('[{}] EXEC SQL DIR: \t {}'.format(time.ctime(),exec_dir))

        os.makedirs( path_tmp_dir, exist_ok=True)
        print('[{}] TMP SQL DIR: \t {}'.format(time.ctime(),tmp_dir))
        
     
    except Exception as e:
        print('Processed records exception.', e)
        raise e

def iterate_file(config):

    flow_choice = sys.argv[1] #take first argument from CLI, e.g. 'python3 snowflake_sql_executor.py core_dl' , the first_argument = core_dl
    
    sql_dir = config[flow_choice + "_dir"]
    tmp_dir = config[flow_choice + "_tmp_dir"]
    exec_dir = config[flow_choice + "_exec_dir"]
    ep_df = iterate_line(config).reset_index()
    
    path_sql_dir = os.getcwd() + '/' + sql_dir
    path_tmp_dir = os.getcwd() + '/' + tmp_dir
    path_exec_dir = os.getcwd() + '/' + exec_dir
    
    export_to_s3_dir = config["export_to_s3_dir"]
    export_file_format = config["export_file_format"]
    
    #copy files from sql_dir to tmp_dir where lines with '#{' are replaced
    for folder, dirs, files in os.walk(path_sql_dir):
        for file in files:
            if file.endswith('.sql'):
                shutil.copyfile(path_sql_dir + file, path_tmp_dir + file)
    
    for folder, dirs, files in os.walk(path_tmp_dir):
        #replace liquid with sql
        for file in files:
            if file.endswith('.sql'):
                fullpath = os.path.join(folder, file)
                for index, row in ep_df.iterrows():
                    if file == row['file']:
                        with open(fullpath) as f:
                            for line in f.readlines():
                                if line == row['line']:

                                    with open(fullpath) as f:
                                        s = f.read()
                                        s = s.replace(line, row['parse_ruby_command']+ '\n')
                                    with open(fullpath, "w") as f:
                                                f.write(s)
		#replace <parameters>
        for file in files:
            if file.endswith('.sql'):
                fullpath = os.path.join(folder, file)                                       
                with open(fullpath) as f:
                    s = f.read()
                    s = s.replace('<export_to_s3_dir>', '{}'.format(export_to_s3_dir))
                    s = s.replace('<export_file_format>', '{}'.format(export_file_format))
                with open(fullpath, "w") as f:
                    f.write(s)


    #copy files from tmp_dir to exec_dir
    for folder, dirs, files in os.walk(path_tmp_dir):
        for file in files:
            if file.endswith('.sql'):
                shutil.copyfile(path_tmp_dir + file, path_exec_dir + file)


def iterate_line(config):

    flow_choice = sys.argv[1] #take first argument from CLI, e.g. 'python3 snowflake_sql_executor.py core_dl' , the first_argument = core_dl
    
    ruby_parser = os.getcwd() + '/' + config["ruby_parser"]
    templates_dir = os.getcwd() + '/' + config["templates_dir"]
    params_dir = os.getcwd() + '/' + config["params_dir"]

    sql_dir = config[flow_choice + "_dir"]
    path_sql_dir = os.getcwd() + '/' + sql_dir

    entity_params = []
    #list = (line, parse_ruby_command)
    
    for folder, dirs, files in os.walk(path_sql_dir):
        for file in files:
            if file.endswith('.sql'):
                path_sql_dir_fullpath = os.path.join(folder, file)


                with open(path_sql_dir_fullpath) as f:
                    for line in f.readlines():
                        if '#{' in line:

                            lq_template_search = re.search('#{(.*)\(', line)
                            params_search = re.search('\(param_definition_file=(.*)\)', line)

                            
                            lq_template = (lq_template_search.group(1) + '.liquid')
                            params = (params_search.group(1))

                            ruby_command = 'ruby' + ' ' + ruby_parser + ' ' + templates_dir + lq_template + ' ' + params_dir + params
                            parse_ruby_command = check_output(ruby_command,shell=True).decode('utf8', errors='strict').strip()

                            #print(parse_ruby_command)
                            #print(line)
                            #print(parse_ruby_command.decode('utf8', errors='strict').strip())
                            list = (file,line, parse_ruby_command)
                            entity_params.append(list)
###
    #print(entity_params)
    ###entity_params.extend([line,ruby_command])
##
    ep_df = pd.DataFrame(entity_params, columns =['file','line', 'parse_ruby_command'])
    #print(ep_df)
    return(ep_df)

    #parent_child_df = pd.DataFrame(parent_child, columns =['fullpath', 'line','lq_template','params'])
    #print(parent_child_df)


def execute_sql(connection, cursor,config):
    
    flow_choice = sys.argv[1] #take first argument from CLI, e.g. 'python3 snowflake_sql_executor.py core_dl' , the first_argument = core_dl
    
    exec_dir = config[flow_choice + "_exec_dir"]
    path_exec_dir = os.getcwd() + '/' + exec_dir
    
    
    print('[{}] Executions of {} sql:\n'.format(time.ctime(),flow_choice))
    
    for folder, dirs, files in os.walk(path_exec_dir):
        sorted_files = sorted( filter( lambda x: os.path.isfile(os.path.join(path_exec_dir, x)),
                            os.listdir(path_exec_dir) ) ) # sorted files in dir by name
        for file in sorted_files:
            fullpath = os.path.join(folder, file)
            sql_list = []
            
            f = open(fullpath, "rt")
            
            # Split the SQL statements into a list
            sql_list = f.read().split(";")
            # Remove any empty statements
            sql_list = [sql.strip() for sql in sql_list if sql.strip()]
            
            #add statements_list to parent sql_list containing all SQLs
            #sql_list.extend(statements_list)
            
            #print(sql_list[0])
            
            for sql in sql_list:
                
                try:
                    cursor.execute(sql)
                    #print("Schema is ready.")
                    print('[{}] \t{} - {} sql executed.\n'.format(time.ctime(),file.replace('.sql',''),flow_choice))
                    
                # cursor.close()
                # connection.close()
                except Exception as e:
                    print("Error while preparing schema.", e)
                    cursor.close()
                    connection.close()
                    raise e

############################################################

def connect_to_db():
    
    s3 = boto3.client('s3')
    config = read_manifest("config.json")
    
    conn = connection(config)
    
    print('\n[{}] --4-- Connect to db and executing SQL\n'.format(time.ctime()))
    
    try:
        global cursor
        cursor = conn.cursor()
        # Print PostgreSQL details
        #print("PostgreSQL server information")
        print(conn._session_parameters, "\n")
        
        if conn._insecure_mode == True:
            print("The connection is in the insecure mode, closing connection.")
        else:
            execute_sql(conn, cursor,config)
        

    except Exception as e:
        print('Processed records exception.', e)
        raise e
    finally:
        if (conn):
            cursor.close()
            conn.close()
        print("Connection is closed")

def connection(config):
    try:
        conn = snowflake.connector.connect(
              account= config["db_account"],
              user= config["db_user"],
              password= config["db_password"],
              warehouse= config["db_warehouse"],
              database= config["db_database"],
              schema= config["db_schema"],
              port= config["db_port"],
              host= config["db_host"],
              autocommit = True,
              sslmode= config["sslmode"],
              sslrootcert= config["sslrootcert"]
        )
        #conn.autocommit = True
        return conn
    except Exception as e:
        print("Invalid connection", e)
        raise e

###########################################################                

############################################################

def main():

    config = read_manifest("config.json")

    create_dirs(config)
    #generate_sqls_from_lq_templates(config)
    iterate_file(config)
    #iterate_line(config)

    connect_to_db()

############################################################
if __name__ == "__main__":
    main()