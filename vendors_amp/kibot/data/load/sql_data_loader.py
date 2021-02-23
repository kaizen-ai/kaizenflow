from typing import Optional
import functools

import pandas as pd
import psycopg2
import psycopg2.extensions as pexten

import helpers.cache as hcache
import helpers.dbg as dbg
import vendors_amp.kibot.data.load.data_loader as vkdlda
import vendors_amp.common.data.types as vkdtyp


class SQLKibotDataLoader(vkdlda.AbstractKibotDataLoader):
    def __init__(
        self, dbname: str, user: str, password: str, host: str, port: int
    ):
        self.conn: psycopg2.extensions.connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )

    # TODO(plyq): Uncomment once #1047 will be resolved.
    # @hcache.cache
    # Use lru_cache for now.
    @functools.lru_cache(maxsize=64)
    def read_data(
        self,
        exchange: str,
        symbol: str,
        asset_class: vkdtyp.AssetClass,
        frequency: vkdtyp.Frequency,
        contract_type: Optional[vkdtyp.ContractType] = None,
        unadjusted: Optional[bool] = None,
        nrows: Optional[int] = None,
        normalize: bool = True,
    ) -> pd.DataFrame:
        """
        Read kibot data.

        :param exchange: name of the exchange
        :param symbol: symbol to get the data for
        :param asset_class: asset class
        :param frequency: `D` or `T` for daily or minutely data respectively
        :param contract_type: required for asset class of type: `futures`
        :param unadjusted: required for asset classes of type: `stocks` & `etfs`
        :param nrows: if not None, return only the first nrows of the data
        :param normalize: whether to normalize the dataframe by frequency
        :return: a dataframe with the symbol data
        """
        return self._read_data(
            exchange=exchange,
            symbol=symbol,
            frequency=frequency,
            nrows=nrows,
        )

    def get_exchange_id(
        self,
        exchange: str,
    ) -> int:
        """
        Get primary key (id) of the Exchange entry by its name.

        :param exchange: Name of the Exchange entry as defined in DB.
        :return: primary key (id)
        """
        exchange_id = -1
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(
                    "SELECT id FROM Exchange WHERE name = %s", [exchange]
                )
                if curs.rowcount:
                    (_exchange_id,) = curs.fetchone()
                    exchange_id = _exchange_id
        if exchange_id == -1:
            dbg.dfatal(f"Could not find Exchange ${exchange}")
        return exchange_id

    def get_symbol_id(
        self,
        symbol: str,
    ) -> int:
        """
        Get primary key (id) of the Symbol entry by its symbol code.

        :param symbol: symbol code, e.g. GOOGL
        :return: primary key (id)
        """
        symbol_id = -1
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute("SELECT id FROM Symbol WHERE code = %s", [symbol])
                if curs.rowcount:
                    (_symbol_id,) = curs.fetchone()
                    symbol_id = _symbol_id
        if symbol_id == -1:
            dbg.dfatal(f"Could not find Symbol ${symbol}")
        return symbol_id

    def get_trade_symbol_id(
        self,
        symbol_id: int,
        exchange_id: int,
    ) -> int:
        """
        Get primary key (id) of the TradeSymbol entry by its respective Symbol
        and Exchange ids.

        :param symbol_id: id of Symbol
        :param exchange_id: id of Exchange
        :return: primary key (id)
        """
        trade_symbol_id = -1
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(
                    "SELECT id FROM TradeSymbol "
                    "WHERE symbol_id = %s AND exchange_id = %s",
                    [symbol_id, exchange_id],
                )
                if curs.rowcount:
                    (_trade_symbol_id,) = curs.fetchone()
                    trade_symbol_id = _trade_symbol_id
        if trade_symbol_id == -1:
            dbg.dfatal(
                f"Could not find Trade Symbol with "
                f"symbol_id={symbol_id} and exchange_id={exchange_id}"
            )
        return trade_symbol_id

    @staticmethod
    def _get_table_name_by_frequency(frequency: vkdtyp.Frequency) -> str:
        """
        Get table name by predefined frequency.

        :param frequency: a predefined frequency
        :return: table name in DB
        """
        table_name = ""
        if frequency == vkdtyp.Frequency.Minutely:
            table_name = "MinuteData"
        elif frequency == vkdtyp.Frequency.Daily:
            table_name = "DailyData"
        elif frequency == vkdtyp.Frequency.Tick:
            table_name = "TickData"
        dbg.dassert(table_name, f"Unknown frequency {frequency}")
        return table_name

    def _read_data(
        self,
        exchange: str,
        symbol: str,
        frequency: vkdtyp.Frequency,
        nrows: Optional[int] = None,
    ) -> pd.DataFrame:
        exchange_id = self.get_exchange_id(exchange)
        symbol_id = self.get_symbol_id(symbol)
        trade_symbol_id = self.get_trade_symbol_id(symbol_id, exchange_id)
        table_name = self._get_table_name_by_frequency(frequency)
        limit = pexten.AsIs("ALL")
        if nrows:
            dbg.dassert_lte(1, nrows)
            limit = nrows
        query = "SELECT * FROM %s WHERE trade_symbol_id = %s LIMIT %s"
        df = pd.read_sql_query(
            query,
            self.conn,
            params=[pexten.AsIs(table_name), trade_symbol_id, limit],
        )
        return df
