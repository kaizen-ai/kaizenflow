r"""
Base methods to run converters.
"""

from typing import Any, Callable, List, Optional, Dict
import logging
import joblib
import tqdm

import helpers.dbg as dbg
import helpers.printing as prnt
import vendors_amp.common.data.types as vkdtyp
import vendors_amp.common.sql_writer_backend as vksqlw
import vendors_amp.common.data.load.s3_data_loader as mds3
import vendors_amp.common.data.load.sql_data_loader as mdsql
import vendors_amp.common.data.transform.s3_to_sql_transformer as mtra

_LOG = logging.getLogger(__name__)

_JOBLIB_NUM_CPUS = 10
_JOBLIB_VERBOSITY = 1

# #############################################################################


def convert_s3_to_sql(
    symbol: str,
    exchange: str,
    s3_data_loader: mds3.AbstractS3DataLoader,
    sql_writer_backend: vksqlw.AbstractSQLWriterBackend,
    sql_data_loader: mdsql.AbstractSQLDataLoader,
    s3_to_sql_transformer: mtra.AbstractS3ToSqlTransformer,
    asset_class: vkdtyp.AssetClass,
    frequency: vkdtyp.Frequency,
    exchange_id: int,
    contract_type: Optional[vkdtyp.ContractType] = None,
    unadjusted: Optional[bool] = None,
    max_num_rows: Optional[int] = None,
) -> bool:
    """
    Convert a dataset from S3 for a symbol.

    :return: True if it was processed
    """
    _LOG.debug("Managing database for '%s' symbol", symbol)
    sql_writer_backend.ensure_symbol_exists(symbol=symbol, asset_class=asset_class)
    symbol_id = sql_data_loader.get_symbol_id(symbol=symbol)
    sql_writer_backend.ensure_trade_symbol_exists(
        symbol_id=symbol_id, exchange_id=exchange_id
    )
    trade_symbol_id = sql_data_loader.get_trade_symbol_id(
        symbol_id=symbol_id, exchange_id=exchange_id
    )
    _LOG.info("Converting '%s' symbol", symbol)
    _LOG.debug("Downloading '%s' symbol from S3", symbol)
    df = s3_data_loader.read_data(
        exchange=exchange,
        symbol=symbol,
        asset_class=asset_class,
        frequency=frequency,
        contract_type=contract_type,
        unadjusted=unadjusted,
        normalize=False,
    )
    if max_num_rows:
        df = df.head(max_num_rows)
    _LOG.debug("Transforming '%s' data before saving to database", symbol)
    df = s3_to_sql_transformer.transform(df, trade_symbol_id=trade_symbol_id, frequency=frequency)
    _LOG.debug("Saving '%s' data to database", symbol)
    if frequency == vkdtyp.Frequency.Minutely:
        sql_writer_backend.insert_bulk_minute_data(df)
    elif frequency == vkdtyp.Frequency.Daily:
        sql_writer_backend.insert_bulk_daily_data(df)
    elif frequency == vkdtyp.Frequency.Tick:
        for _, row in df.iterrows():
            sql_writer_backend.insert_tick_data(
                trade_symbol_id=row["trade_symbol_id"],
                date_time=row["datetime"],
                price_val=row["price"],
                size_val=row["size"],
            )
    else:
        dbg.dfatal("Unknown frequency '%s'", frequency)
    _LOG.info("Done converting '%s' symbol", symbol)
    # Return info about loaded data.
    loaded_data = sql_data_loader.read_data(exchange=exchange, symbol=symbol, asset_class=asset_class, frequency=frequency, contract_type=contract_type, unadjusted=unadjusted, nrows=None, normalize=True)
    _LOG.info("Total %s records loaded for symbol '%s'", len(loaded_data), symbol)
    _LOG.debug("Tail of loaded data:\n%s", prnt.frame(loaded_data.tail().to_string()))
    return True


def convert_s3_to_sql_bulk(
    serial: bool, params_list: List[Dict[str, Any]]
) -> None:
    """
    Process in parallel each set of params in the list.

    :param serial: whether to run sequentially
    :param params_list: list of parameters to run
    """
    tqdm_ = tqdm.tqdm(params_list, desc="symbol", total=len(params_list))
    if serial:
        for params in tqdm_:
            convert_s3_to_sql(**params)
    else:
        joblib.Parallel(
            n_jobs=_JOBLIB_NUM_CPUS,
            verbose=_JOBLIB_VERBOSITY,
            require="sharedmem",
        )(joblib.delayed(convert_s3_to_sql)(**params) for params in tqdm_)


