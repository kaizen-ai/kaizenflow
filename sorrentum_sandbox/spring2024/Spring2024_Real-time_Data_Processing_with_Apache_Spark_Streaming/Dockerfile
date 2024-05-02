FROM jupyter/pyspark-notebook:latest

EXPOSE 8888

ENV SPARK_HOME=/usr/local/spark

ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
ENV PATH=$SPARK_HOME/bin:$PATH

RUN pip install py4j

COPY main_app.ipynb /home/jovyan/

CMD ["start-notebook.sh", "--NotebookApp.token=''"]
