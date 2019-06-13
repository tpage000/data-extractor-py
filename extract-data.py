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

print('extract data')

