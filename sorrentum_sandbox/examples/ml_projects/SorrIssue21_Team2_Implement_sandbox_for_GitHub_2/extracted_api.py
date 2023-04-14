#!/usr/bin/env python3
##Import requried packages
import pandas as pd
import psycopg2 as psycop



#Connection String for main DB
def get_db_connection(query_var) :       
    connection = psycop.connect(
        host="host.docker.internal",                                      
        dbname="airflow",
        port=5432,
        user="postgres",
        password="postgres")
    drt_cursor=connection.cursor()
    drt_cursor.execute(query_var)
    data=drt_cursor.fetchall()
    connection.close()
    return pd.DataFrame(data)

