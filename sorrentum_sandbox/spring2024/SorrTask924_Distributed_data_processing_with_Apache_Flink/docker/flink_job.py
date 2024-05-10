import json
from typing import Tuple

from pyflink.common import SimpleStringSchema, WatermarkStrategy, Types, Time, Row
from pyflink.datastream import StreamExecutionEnvironment, MapFunction, AggregateFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.datastream.window import TumblingEventTimeWindows

from pyflink.table import *
from pyflink.table.expressions import call, col, lit
from pyflink.table.window import Tumble


APP_NAME = 'STREAMING_API_AGGREGATE_KAFKA'
KAFKA_SERVERS = 'kafka:9092'
KAFKA_SOURCE_TOPIC = 'client_amount'
KAFKA_TARGET_TOPIC = 'client_total_amount'
KAFKA_CONSUMER = 'Flink1'

class ParseMapFunction(MapFunction):

    def map(self, data):
        json_data = json.loads(data)
        return json_data['client'], json_data['amount']


class SumAggregate(AggregateFunction):

    def create_accumulator(self) -> Tuple[int, int]:
        return 0, 0

    def add(self, value: Tuple[int, int], accumulator: Tuple[int, int]) -> Tuple[int, int]:
        return value[0], accumulator[1] + value[1]

    def get_result(self, accumulator: Tuple[int, int]) -> Tuple[int, int]:
        return Row(accumulator[0], accumulator[1])

    def merge(self, a: Tuple[int, int], b: Tuple[int, int]) -> Tuple[int, int]:
        return a[0], a[1] + b[1]


def process_json_data():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

    create_kafka_source_ddl = """
            CREATE TABLE iot_msg(
                client INT,
                amount INT,
                ts TIMESTAMP(3),
                WATERMARK FOR ts AS ts
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'client_amount',
              'properties.bootstrap.servers' = 'kafka:9092',
              'properties.group.id' = 'test_3',
              'scan.startup.mode' = 'latest-offset',
              'format' = 'json'
            )
            """

    create_es_sink_ddl = """
            CREATE TABLE es_sink(
                client INT PRIMARY KEY NOT ENFORCED,
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3),
                total_amount INT
            ) with (
                'connector' = 'elasticsearch-7',
                'hosts' = 'http://elasticsearch:9200',
                'index' = 'total_amount_1',
                'document-id.key-delimiter' = '$',
                'sink.bulk-flush.max-size' = '42mb',
                'sink.bulk-flush.max-actions' = '32',
                'sink.bulk-flush.interval' = '1000',
                'sink.bulk-flush.backoff.delay' = '1000',
                'format' = 'json'
            )
    """

    t_env.execute_sql(create_kafka_source_ddl)
    t_env.execute_sql(create_es_sink_ddl)

    iot_data = t_env.from_path("iot_msg") 

    results = iot_data.window(Tumble.over(lit(1).minutes).on(col("ts")).alias("w")) \
        .group_by(col("w"), col("client")) \
        .select(col("client"), col("w").start.alias("window_start"), col("w").end.alias("window_end"), col("amount").sum.alias("total_amount")) \
        .execute_insert("es_sink")

    # env.execute_async(APP_NAME)


if __name__ == '__main__':
    process_json_data()