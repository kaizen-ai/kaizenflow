import json
import logging
from confluent_kafka import KafkaError, Consumer
import psycopg2

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class Env:
    def __init__(self, conf):
        self.conf = conf
        self.consumer = Consumer(conf)


def create_database(conn_params, dbname):
    conn_params_default = conn_params.copy()
    conn_params_default["dbname"] = "postgres"

    # Connect to the default database (postgres) to issue commands
    conn = psycopg2.connect(**conn_params_default)
    # Set the connection to autocommit mode
    conn.autocommit = True
    cur = conn.cursor()

    # Check if the database already exists
    cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (dbname,))
    exists = cur.fetchone()

    if exists:
        print(f"Database '{dbname}' already exists.")
    else:
        # Execute the command to create a new database
        try:
            cur.execute(
                f"CREATE DATABASE {dbname};"
            )  # Using f-string for database name in SQL statement
            print(f"Database '{dbname}' created successfully.")
        except psycopg2.Error as e:
            print(f"An error occurred: {e}")

    cur.close()
    conn.close()


def create_table(conn_params, table_name):
    """
    Creates a table in the PostgreSQL database with a predefined schema.

    Parameters:
    conn_params (dict): A dictionary containing the connection parameters.
    table_name (str): The name of the table to create.
    """
    try:
        # Establish the connection using the connection parameters
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                # SQL command to create a table
                create_table_command = f"""
					CREATE TABLE IF NOT EXISTS {table_name} (
						event_type TEXT,
						event_time BIGINT,
						symbol TEXT,
						trade_id BIGINT,
						price NUMERIC,
						quantity NUMERIC,
						buyer_order_id BIGINT,
						seller_order_id BIGINT,
						trade_time BIGINT,
						is_buyer_maker BOOLEAN,
						ignore_in_price BOOLEAN
					);
				"""
                # Execute the create table command
                cur.execute(create_table_command)
                # Commit the transaction
                conn.commit()
                print(f"Table '{table_name}' created successfully")

    except psycopg2.Error as e:
        # Handle exceptions that occur during the creation of the table
        print(f"Failed to create table: {e}")


def consume_from_kafka(env, topic_name):
    try:
        consumer = env.consumer
        consumer.subscribe([topic_name])
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cursor:
                print("Starting the consumer loop.")
                # Consumer loop
                while True:
                    # Poll for a new message
                    msg = consumer.poll(timeout=1.0)  # Poll timeout in seconds

                    if msg is None:
                        continue  # No message available within the timeout period

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition event
                            print(
                                f"Reached end of {msg.partition()} at offset {msg.offset()}"
                            )
                        else:
                            print(f"Error: {msg.error()}")
                        continue

                    # process_message
                    message_data = json.loads(msg.value().decode("utf-8"))
                    print(
                        f"Received message: {message_data} from partition {msg.partition()}"
                    )
                    insert_query = """
						INSERT INTO binance 
							(
								event_type, event_time, symbol, trade_id, price, quantity, buyer_order_id, 
								seller_order_id, trade_time, is_buyer_maker, ignore_in_price
							)
						VALUES (%(e)s, %(E)s, %(s)s, %(t)s, %(p)s, %(q)s, %(b)s, %(a)s, %(T)s, %(m)s, %(M)s);
					"""
                    cursor.execute(insert_query, message_data)
                    print("Record inserted")
                    conn.commit()

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    finally:
        print("Closing the consumer.")
        consumer.close()


if __name__ == "__main__":

    conf = {
        "bootstrap.servers": "broker:9092",
        "group.id": "group1",
        "auto.offset.reset": "earliest",
    }
    env = Env(conf)
    topic_name = "trades"

    conn_params = {
        "dbname": "trades",
        "user": "postgres",
        "password": "postgres",
        "host": "pgdatabase",
        "port": "5432",
    }
    create_database(conn_params, "trades")
    create_table(conn_params, "binance")
    consume_from_kafka(env, topic_name)
