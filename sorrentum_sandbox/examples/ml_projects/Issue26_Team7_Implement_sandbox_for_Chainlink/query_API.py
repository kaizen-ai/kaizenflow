import psycopg2
import pandas as pd

def query_from_db(query):
    connection = psycopg2.connect(
            host="host.docker.internal",
            dbname="airflow",
            port=5532,
            user="postgres",
            password="postgres",
        )
    cursor = connection.cursor()
    cursor.execute(query)
    history_rows = cursor.fetchall()
    df = pd.DataFrame(history_rows, columns=[desc[0] for desc in cursor.description])

    cursor.close()
    connection.close()

    return df