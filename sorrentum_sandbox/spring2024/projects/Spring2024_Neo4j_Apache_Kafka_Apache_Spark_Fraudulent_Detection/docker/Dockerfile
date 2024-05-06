FROM python:3.10.12

WORKDIR /app

COPY . .

RUN chmod 777 /app/Sparkov_Data_Generation/datagen_command.sh; chmod 777 /app/start.sh

RUN pip install -r requirements.txt

RUN apt-get update

RUN apt install -y default-jdk

RUN cp neo4j-connector-apache-spark_2.12-5.3.0_for_spark_3.jar /usr/local/lib/python3.10/site-packages/pyspark/jars/neo4j-connector-apache-spark_2.12-5.3.0_for_spark_3.jar

RUN /usr/local/lib/python3.10/site-packages/pyspark/bin/spark-shell --jars /usr/local/lib/python3.10/site-packages/pyspark/bin/spark-shell/jars/neo4j-connector-apache-spark_2.12-5.3.0_for_spark_3.jar

RUN apt-get -y install cron

COPY cronjob /etc/cron.d/cronjob

RUN crontab /etc/cron.d/cronjob

EXPOSE 8888

EXPOSE 7687

EXPOSE 4040

CMD ["/app/start.sh"]
