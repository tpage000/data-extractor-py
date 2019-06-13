from pymongo import MongoClient
import pandas as pd
import datetime
import sys
from dotenv import load_dotenv
import os
load_dotenv()

# OPTIONS
# --dev     --print/--csv/--count
# --test    --print/--csv/--count
# --prod    --count

def main():
    env_flag_list = ['--dev', '--test', '--prod']
    output_flag_list = ['--print', '--count', '--csv']
    return (
        print_args_error('Missing arguments') if len(sys.argv) < 3
        else print_args_error('Incorrect env arguments') if not sys.argv[1] in env_flag_list
        else print_args_error('Incorrect output arguments') if not sys.argv[2] in output_flag_list
        else print_args_error('Cannot print or save prod data') if sys.argv[1] == '--prod' and sys.argv[2] != '--count'
        else set_connection(sys.argv[1], sys.argv[2]))

def set_connection(env_flag, output_flag):
    print(env_flag, output_flag)

def print_args_error(message):
    print(f'{message}. Please supply: ')
    print('First argument: environment')
    print('--dev')
    print('--test')
    print('--prod')
    print('Second argument: output format')
    print('--count')
    print('--print (not for prod)')
    print('--csv (not for prod)')

main()
