# API Query for postgres db
import pandas as pd
from sqlalchemy import create_engine, text

def api_query_call(query: str) -> pd.DataFrame:
    # Create a SQLAlchemy engine
    engine = create_engine("postgresql://postgres:postgres@host.docker.internal:5532/airflow")


    df = pd.DataFrame(engine.connect().execute(text(query)))
    # Disconnect from the database
    engine.dispose()
    return df


def save_table(table_name:str, df: pd.DataFrame):
    # Create a SQLAlchemy engine
    engine = create_engine("postgresql://postgres:postgres@host.docker.internal:5532/airflow")

    df.to_sql('query_calc',engine, if_exists='replace',index=False)