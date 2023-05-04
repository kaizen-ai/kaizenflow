import pandas
import dask.dataframe as dd
import load_data

def work(date):
    df = load_data.realtime_from_csv()
    # use Dask
    df = dd.from_pandas(df, chunksize=4)
    df.drop_duplicates(subset=["timestamp", "trade_id", "amount"], keep="last", inplace=True)
    # select data
    df['timestamp'] = pandas.to_datetime(df['timestamp'], infer_datetime_format=True)
    new_table = df[df['timestamp'].dt.date == date.date()]
    return new_table.compute()

if __name__ == "__main__":
    print(work(pandas.to_datetime("2023-4-1")))