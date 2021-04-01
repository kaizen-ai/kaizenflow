import pytest

import helpers.unit_test as hut
import vendors_amp.common.data.transform.transform as vcdttr
import vendors_amp.common.data.types as vcdtyp
import vendors_amp.common.db.init as vcdini
import vendors_amp.common.test.utils as vctuti
import vendors_amp.ib.data.load.s3_data_loader as vidls3
import vendors_amp.ib.data.load.sql_data_loader as vidlsq
import vendors_amp.ib.data.transform.s3_to_sql_transformer as vidts3
import vendors_amp.ib.sql_writer_backend as visqlw


@pytest.mark.skipif(
    not vcdini.is_inside_im_container(),
    reason="Testable only inside IM container",
)
class TestIbEndToEnd(vctuti.SqlWriterBackendTestCase):
    """
    Test end to end operations for IM provider.
    """

    def setUp(self) -> None:
        super().setUp()
        self._dbname_test = "im_postgres_db_local"
        # Initialize writer class to test.
        self._writer = visqlw.SQLWriterIbBackend(
            dbname=self._dbname_test,
            user=self._user,
            password=self._password,
            host=self._host,
            port=self._port,
        )

    def test_insert_daily_data_from_s3(self) -> None:
        exchange = "NYSE"
        symbol = "ES"
        s3_data_loader = vidls3.S3IbDataLoader()
        s3_to_sql_transformer = vidts3.S3ToSqlIbTransformer()
        sql_data_loader = vidlsq.SQLIbDataLoader(
            dbname=self._dbname_test,
            user=self._user,
            password=self._password,
            host=self._host,
            port=self._port,
        )
        self._writer.ensure_symbol_exists(
            symbol=symbol, asset_class=vcdtyp.AssetClass.Futures
        )
        self._writer.ensure_exchange_exists(exchange)
        exchange_id = sql_data_loader.get_exchange_id(exchange)
        params_list = dict(
            symbol=symbol,
            max_num_rows=40,
            s3_data_loader=s3_data_loader,
            sql_writer_backend=self._writer,
            sql_data_loader=sql_data_loader,
            s3_to_sql_transformer=s3_to_sql_transformer,
            asset_class=vcdtyp.AssetClass.Futures,
            contract_type=vcdtyp.ContractType.Continuous,
            frequency=vcdtyp.Frequency.Daily,
            unadjusted=True,
            exchange_id=exchange_id,
            exchange=exchange,
        )
        vcdttr.convert_s3_to_sql(**params_list)
        df = sql_data_loader.read_data(
            exchange=exchange,
            symbol=symbol,
            asset_class=vcdtyp.AssetClass.Futures,
            frequency=vcdtyp.Frequency.Daily,
            contract_type=vcdtyp.ContractType.Continuous,
        )
        # Convert dataframe to string.
        df.drop(columns=["id"], inplace=True)
        txt = hut.convert_df_to_string(df)
        # Check the output against the golden.
        self.check_string(txt, fuzzy_match=True)

    def test_insert_minutely_data_from_s3(self) -> None:
        exchange = "NYSE"
        symbol = "ES"
        frequency = vcdtyp.Frequency.Minutely
        s3_data_loader = vidls3.S3IbDataLoader()
        s3_to_sql_transformer = vidts3.S3ToSqlIbTransformer()
        sql_data_loader = vidlsq.SQLIbDataLoader(
            dbname=self._dbname_test,
            user=self._user,
            password=self._password,
            host=self._host,
            port=self._port,
        )
        self._writer.ensure_symbol_exists(
            symbol=symbol, asset_class=vcdtyp.AssetClass.Futures
        )
        self._writer.ensure_exchange_exists(exchange)
        exchange_id = sql_data_loader.get_exchange_id(exchange)
        params_list = dict(
            symbol=symbol,
            max_num_rows=20,
            s3_data_loader=s3_data_loader,
            sql_writer_backend=self._writer,
            sql_data_loader=sql_data_loader,
            s3_to_sql_transformer=s3_to_sql_transformer,
            asset_class=vcdtyp.AssetClass.Futures,
            contract_type=vcdtyp.ContractType.Continuous,
            frequency=frequency,
            unadjusted=True,
            exchange_id=exchange_id,
            exchange=exchange,
        )
        vcdttr.convert_s3_to_sql(**params_list)
        df = sql_data_loader.read_data(
            exchange=exchange,
            symbol=symbol,
            asset_class=vcdtyp.AssetClass.Futures,
            frequency=frequency,
            contract_type=vcdtyp.ContractType.Continuous,
        )
        # Convert dataframe to string.
        df.drop(columns=["id"], inplace=True)
        txt = hut.convert_df_to_string(df)
        # Check the output against the golden.
        self.check_string(txt, fuzzy_match=True)
