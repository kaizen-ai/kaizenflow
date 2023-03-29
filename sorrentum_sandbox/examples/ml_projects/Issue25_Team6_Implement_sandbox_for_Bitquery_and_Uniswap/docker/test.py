
from typing import Any, Dict, List

import os
import pandas as pd
import requests
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime
from io import StringIO
# API Key is BQYfQWbIU9aPXWtxpMAixOrC1fCLiIz3


# database connection parameters
host = 'localhost'
port = '5432' # this might be 8001
dbname = 'db'
user = 'user'
password = 'password'

# connection to the postgress database
conn = psycopg2.connect(
    host=host,
    port=port,
    dbname=dbname,
    user=user,
    password=password
)


# Create a cursor to execute SQL queries
cur = conn.cursor()
# Execute a SQL query to retrieve the last row of the table
cur.execute("SELECT * FROM tran_metadata")
result = cur.fetchall()
df = pd.DataFrame(result)
print(df.head())
