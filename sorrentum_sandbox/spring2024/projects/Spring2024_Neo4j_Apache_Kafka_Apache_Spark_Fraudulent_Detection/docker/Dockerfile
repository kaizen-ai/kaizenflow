FROM python:3.10.12

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

RUN apt-get update

RUN apt install -y default-jdk

RUN cp neo4j-connector-apache-spark_2.12-5.3.0_for_spark_3.jar /usr/local/lib/python3.10/site-packages/pyspark/neo4j-connector-apache-spark_2.12-5.3.0_for_spark_3.jar

RUN cp neo4j-connector-apache-spark_2.12-5.3.0_for_spark_3.jar /usr/local/lib/python3.10/site-packages/pyspark/jars/neo4j-connector-apache-spark_2.12-5.3.0_for_spark_3.jar

RUN /usr/local/lib/python3.10/site-packages/pyspark/bin/spark-shell --jars neo4j-connector-apache-spark_2.12-5.3.0_for_spark_3.jar

RUN /usr/local/lib/python3.10/site-packages/pyspark/bin/spark-shell --jars /usr/local/lib/python3.10/site-packages/pyspark/bin/spark-shell/jars/neo4j-connector-apache-spark_2.12-5.3.0_for_spark_3.jar

EXPOSE 8888

EXPOSE 7687

CMD ["jupyter","notebook", "--no-browser", "--allow-root","--ip=0.0.0.0", "--port=8888"]
