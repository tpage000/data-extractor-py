from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
import math
import ast
import sys
import os
load_dotenv()

def main():
    env_flag_list = ['--dev', '--test', '--prod']
    output_flag_list = ['--print', '--count', '--csv']
    query = sys.argv[3] if len(sys.argv) >= 4 else "{}"
    fields = sys.argv[4] if len(sys.argv) >= 5 else None
    return (
        print_args_error('Missing arguments') if len(sys.argv) < 3
        else print_args_error('Incorrect env arguments') if not sys.argv[1] in env_flag_list
        else print_args_error('Incorrect output arguments') if not sys.argv[2] in output_flag_list
        else print_args_error('Cannot print or save prod data') if sys.argv[1] == '--prod' and sys.argv[2] != '--count'
        else set_connection(sys.argv[1], sys.argv[2], query, fields))

def set_connection(env_flag, output_flag, query, fields):
    return (
        connect_to_db(
            os.getenv('DEV_DB_CONNECTION'), os.getenv('DEV_DB_NAME'), 
            os.getenv('DEV_DB_COLLECTION'), env_flag, output_flag, query, fields) if env_flag == '--dev'
        else connect_to_db(
            os.getenv('TEST_DB_CONNECTION'), os.getenv('TEST_DB_NAME'), 
            os.getenv('TEST_DB_COLLECTION'), env_flag, output_flag, query, fields) if env_flag == '--test'
        else connect_to_db(
            os.getenv('PROD_DB_CONNECTION'), os.getenv('PROD_DB_NAME'), 
            os.getenv('PROD_DB_COLLECTION'), env_flag, output_flag, query, fields) if env_flag == '--prod'
        else print_args_error('Missing or incorrect env arguments'))

def connect_to_db(connection_string, db_name, collection_name, env_flag, output_flag, query, fields):
    connection = MongoClient(connection_string)
    db = connection[db_name][collection_name]
    return (
        count_results(connection, db, query) if output_flag == '--count'
        else extract_and_process(connection, db, env_flag, output_flag, query, fields))

def count_results(connection, db, query):
    print(db.count_documents( ast.literal_eval(query) ))
    return connection.close()

def extract_and_process(connection, db, env_flag, output_flag, query, fields):
    if not fields:
        results = db.find(ast.literal_eval(query))
    else:
        results = db.find(ast.literal_eval(query), ast.literal_eval(fields)) 
    dataframe = pd.DataFrame(list(results))
    if '_id' in dataframe: del dataframe['_id']
    if '__v' in dataframe: del dataframe['__v']
    return (
        print_results(connection, dataframe) if output_flag == '--print'
        else create_csv(connection, dataframe, env_flag) if output_flag == '--csv' and env_flag != '--prod'
        else print_args_error('Missing or incorrect output format'))

def print_results(connection, dataframe):
    print('=======')
    print(dataframe)
    print('=======')
    return connection.close()

def create_csv(connection, dataframe, env_flag):
    max_rows_per_file = 30000.0
    path = '../data/'
    timestamp = datetime.now().isoformat()
    total_rows = len(dataframe.index)
    number_of_files = math.ceil(total_rows / max_rows_per_file)
    start_index = 0
    end_index = int(max_rows_per_file)
    iterations_remaining = number_of_files
    create_each_file(
            number_of_files, max_rows_per_file, dataframe, start_index, end_index, 
            timestamp, path, env_flag, iterations_remaining)
    print('Done')
    print(f'Extracted {len(dataframe.index)} documents to: {path}')
    return connection.close()

def create_each_file(number_of_files, max_rows_per_file, dataframe, start_index, end_index, 
        timestamp, path, env_flag, iterations_remaining):
    if iterations_remaining:
        filestring = f'{timestamp}-{number_of_files - iterations_remaining + 1}{env_flag}.csv'
        print(f'writing to {path}{filestring} ........')
        df_slice = dataframe.iloc[ start_index : end_index ]
        df_slice.to_csv(path + filestring, index=False, encoding='utf-8')
        return create_each_file(
                number_of_files, max_rows_per_file, dataframe,
                start_index + max_rows_per_file, end_index + max_rows_per_file, timestamp, 
                path, env_flag, iterations_remaining - 1)

def create_csv_error(connection):
    print('create csv error')
    return connection.close()

def print_args_error(message):
    print(f'{message}. Please supply: ')
    print('First argument: environment')
    print('--dev')
    print('--test')
    print('--prod')
    print('Second argument: output format')
    print('--print (for --dev and --test only)')
    print('--count')
    print('--csv')

