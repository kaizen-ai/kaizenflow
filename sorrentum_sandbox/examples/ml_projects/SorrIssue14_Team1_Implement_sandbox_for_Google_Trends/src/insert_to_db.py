import psycopg2 as psycop
import psycopg2.extras as extras
import db_init
import pandas as pd


def insert(cleaned_data, db_connection):

    values = [tuple(v) for v in cleaned_data.to_numpy()]

    query = f"INSERT INTO google_trends_data VALUES %s"

    cursor = db_connection.cursor()
    extras.execute_values(cursor, query, values)


if __name__ == '__main__':

    db_connection = db_init.get_db_connection()
    cleaned_data = pd.read_csv("../data/cleaned_data.csv")

    insert(cleaned_data, db_connection)

    db_connection.close()

