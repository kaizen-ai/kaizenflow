
from api_query import api_query_call
import sorrentum_sandbox.examples.ml_projects.Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap.db as sisebidb
import pandas as pd
import sorrentum_sandbox.common.download as ssandown
import dask.dataframe as dd

# Load dataframe from uniswap_table
query = "SELECT * FROM uniswap_table"
df_pandas = api_query_call(query)

# Clear previous query_results
api_query_call("DROP TABLE query_calc")

# Use DASK to partition pandas dataframe for faster processing
df = dd.from_pandas(df_pandas, npartitions=10)  

# We can see the highest trading wallets
counts = df[['transaction_to_address', 'transaction_txfrom_address']].stack().value_counts()
raw_data = counts

# # Print the wallet with the highest transactions
# print(counts)

table_name = "query_calc"

raw_data = ssandown.RawData(counts)
# Save data to DB.
db_conn = sisebidb.get_db_connection()
saver = sisebidb.PostgresDataFrameSaver(db_conn)
saver.save(raw_data, table_name)