from typing import Any

import psycopg2 as psycop


def get_db_connection() -> Any:
    """
    Retrieve connection to the Postgres DB inside the Sorrentum data node.

    The parameters must match the parameters set up in the Sorrentum
    data node docker-compose.
    """
    connection = psycop.connect(
        host="localhost",
        dbname="postgres",
        port=5432,
        user="postgres",
        password="postgres",
    )
    connection.autocommit = True

    return connection


def get_google_trends_create_table_query() -> str:
    """
    Get SQL query to create google trends table.
    """
    query = """
                CREATE TABLE IF NOT EXISTS google_trends_data
                (
                    topic VARCHAR(225), 
                    date_stamp varchar(225), 
                    frequency NUMERIC
                );
            """
    return query


def get_google_trends_table_drop_query() -> str:
    """
    Get SQL query to drop google trends table.
    """
    query = "drop table google_trends_data"

    return query


def get_google_trends_table_fetch_query(topic) -> str:

    query = ""
    query = query + "SELECT * FROM google_trends_data WHERE topic = "
    query = query + ("'" + topic + "'")

    return query


if __name__ == "__main__":
    connection_obj = get_db_connection()
    print("Connection object: ", connection_obj)

    table_creation_query = get_google_trends_create_table_query()
    table_deletion_query = get_google_trends_table_drop_query()

    print("query to print table: \n", table_creation_query)

    cursor = connection_obj.cursor()
    cursor.execute(table_creation_query)

    connection_obj.close()
    print("connection closed")
