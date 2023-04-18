import pandas as pd
import db_init


def fetch_from_db(db_connection, topic):
    fetch_query = db_init.get_google_trends_table_fetch_query(topic)
    fetched_data = pd.read_sql_query(fetch_query, db_connection)

    return fetched_data


if __name__ == "__main__":
    db_connection = db_init.get_db_connection()
    data = fetch_from_db(db_connection, "summer")

    print(data)

