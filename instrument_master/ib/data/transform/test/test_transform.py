import pytest

import helpers.unit_test as hut
import instrument_master.common.data.transform.transform as vcdttr
import instrument_master.common.data.types as vcdtyp
import instrument_master.common.db.init as vcdini
import instrument_master.common.test.utils as vctuti
import instrument_master.ib.data.load.s3_data_loader as vidls3
import instrument_master.ib.data.load.sql_data_loader as vidlsq
import instrument_master.ib.data.transform.s3_to_sql_transformer as vidts3
import instrument_master.ib.sql_writer_backend as visqlw


@pytest.mark.skipif(
    not vcdini.is_inside_im_container(),
    reason="Testable only inside IM container",
)
class TestReadFromS3WriteToSql(vctuti.SqlWriterBackendTestCase):
    """
    Test migrating data from S3 to SQL for IB provider.
    """

    def setUp(self) -> None:
        super().setUp()
        # Initialize writer class to test.
        self._writer = visqlw.IbSqlWriterBackend(
            dbname=self._dbname,
            user=self._user,
            password=self._password,
            host=self._host,
            port=self._port,
        )
        self._s3_data_loader = vidls3.S3IbDataLoader()
        self._s3_to_sql_transformer = vidts3.S3ToSqlIbTransformer()
        self._sql_data_loader = vidlsq.SQLIbDataLoader(
            dbname=self._dbname,
            user=self._user,
            password=self._password,
            host=self._host,
            port=self._port,
        )

    def tearDown(self) -> None:
        self._sql_data_loader.close()
        super().tearDown()

    def test_insert_daily_data_from_s3_1(self) -> None:
        """
        Test equal with:

        ```
        > app/instument_master/transform/convert_s3_to_sql.py \
                --provider ib \
                --symbol ES \
                --frequency D \
                --contract_type continuous \
                --asset_class Futures \
                --exchange GLOBEX \
                --max_num_rows 10
        ```
        """
        exchange = "GLOBEX"
        symbol = "ES"
        asset_class = vcdtyp.AssetClass.Futures
        contract_type = contract_type = vcdtyp.ContractType.Continuous
        frequency = vcdtyp.Frequency.Daily
        self._run_and_check_s3_to_sql(
            exchange=exchange,
            symbol=symbol,
            asset_class=asset_class,
            contract_type=contract_type,
            frequency=frequency,
        )

    def test_insert_daily_data_from_s3_2(self) -> None:
        """
        Test equal with:

        ```
        > app/instument_master/transform/convert_s3_to_sql.py \
                --provider ib \
                --symbol HG \
                --frequency D \
                --contract_type continuous \
                --asset_class Futures \
                --exchange NYMEX \
                --max_num_rows 10
        ```
        """
        exchange = "NYMEX"
        symbol = "HG"
        asset_class = vcdtyp.AssetClass.Futures
        contract_type = contract_type = vcdtyp.ContractType.Continuous
        frequency = vcdtyp.Frequency.Daily
        self._run_and_check_s3_to_sql(
            exchange=exchange,
            symbol=symbol,
            asset_class=asset_class,
            contract_type=contract_type,
            frequency=frequency,
        )

    def test_insert_minutely_data_from_s3_1(self) -> None:
        """
        Test equal with:

        ```
        > app/instument_master/transform/convert_s3_to_sql.py \
                --provider ib \
                --symbol ES \
                --frequency T \
                --contract_type continuous \
                --asset_class Futures \
                --exchange GLOBEX \
                --max_num_rows 10
        ```
        """
        exchange = "GLOBEX"
        symbol = "ES"
        asset_class = vcdtyp.AssetClass.Futures
        contract_type = contract_type = vcdtyp.ContractType.Continuous
        frequency = vcdtyp.Frequency.Minutely
        self._run_and_check_s3_to_sql(
            exchange=exchange,
            symbol=symbol,
            asset_class=asset_class,
            contract_type=contract_type,
            frequency=frequency,
        )

    def test_insert_minutely_data_from_s3_2(self) -> None:
        """
        Test equal with:

        ```
        > app/instument_master/transform/convert_s3_to_sql.py \
                --provider ib \
                --symbol HG \
                --frequency T \
                --contract_type continuous \
                --asset_class Futures \
                --exchange NYMEX \
                --max_num_rows 10
        ```
        """
        exchange = "NYMEX"
        symbol = "HG"
        asset_class = vcdtyp.AssetClass.Futures
        contract_type = contract_type = vcdtyp.ContractType.Continuous
        frequency = vcdtyp.Frequency.Minutely
        self._run_and_check_s3_to_sql(
            exchange=exchange,
            symbol=symbol,
            asset_class=asset_class,
            contract_type=contract_type,
            frequency=frequency,
        )

    def _run_and_check_s3_to_sql(
        self,
        exchange: str,
        symbol: str,
        asset_class: vcdtyp.AssetClass,
        contract_type: vcdtyp.ContractType,
        frequency: vcdtyp.Frequency,
    ) -> None:
        """
        Run the whole lifecycle of data starting from reading from S3.

        - Read from S3
        - Transform
        - Write to PostgreSQL
        - Read from PostgrSQL
        - Check results

        :param exchange: symbol exchange
        :param symbol: symbol name
        :param asset_class: symbol asset class
        :param contract_type: symbol contract_type
        :param frequency: data frequency to return
        """
        # Prepare database.
        self._writer.ensure_symbol_exists(symbol=symbol, asset_class=asset_class)
        self._writer.ensure_exchange_exists(exchange)
        exchange_id = self._sql_data_loader.get_exchange_id(exchange)
        params_list = dict(
            symbol=symbol,
            max_num_rows=10,
            s3_data_loader=self._s3_data_loader,
            sql_writer_backend=self._writer,
            sql_data_loader=self._sql_data_loader,
            s3_to_sql_transformer=self._s3_to_sql_transformer,
            asset_class=asset_class,
            contract_type=contract_type,
            frequency=frequency,
            unadjusted=True,
            exchange_id=exchange_id,
            exchange=exchange,
        )
        # Read, transform data from S3 and put to the database.
        vcdttr.convert_s3_to_sql(**params_list)
        # Find what was written.
        df = self._sql_data_loader.read_data(
            exchange=exchange,
            symbol=symbol,
            asset_class=asset_class,
            frequency=frequency,
            contract_type=contract_type,
        )
        # Convert dataframe to string.
        # df.drop(columns=["id"], inplace=True)
        txt = hut.convert_df_to_string(df)
        # Check the output against the golden.
        self.check_string(txt, fuzzy_match=True)
