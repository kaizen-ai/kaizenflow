**Technology Used**

The primary technology used in my project, aside from Python and Jupyter 
notebooks, was PySpark, the Python API for accessing Apache Spark.  Apache 
Spark is a framework designed for quickly processing large datasets, and can be
used in conjunction with a wide array of languages (such as Java, Python, and 
R) for a multitude of applications, such as SQL, machine learning, and real-
time data processing (RTDP).  PySpark allows the user to access this powerful
framework.  

Apache Spark is centered around Resilient Distributed Datasets (RDD's 
hereafter), which allow users to split a large (and immutable) dataset between 
multiple machines, thereby working around storage limitations (Pointer, 2024).  
Among the many advantages of Apache Spark are its capability to quickly perform 
advanced analytics, support for multiple programming languages (as stated 
previously), flexibility, and "detailed documentation...[which] provides 
detailed tutorials and examples that explain complex concepts clearly and 
concisely" ("The Good", 2023).  However, with these high performance specs come 
higher computational costs.  Additionally, Apache Spark is known to have 
trouble with larger collections of small files and relies on external storage 
("The Good", 2023).

*Similar Technologies*

Other Apache technologies such as Hadoop, Flink, and Hadoop MapReduce offer 
similar data processing capabilities.  Hadoop handles large datasets more 
effectively than Spark, but at the cost of time ("The Good", 2023).  Flink, 
like Spark, supports multiple programming languages, but Spark is superior in 
terms of ease of use (Mohan & Thyagarajan, 2023).  

Hadoop MapReduce is among the most popular alternatives, but still has some key
differences.  While Hadoop MapReduce performs better with batch processing, 
"Apache Spark is more suited for real-time data processing [the central focus
of this project] and iterative analytics" (Tobin, 2023).  Additionally, as is
the case in the comparison to Flink, Spark is easier to use than MapReduce, 
with a "more user-friendly programming interface" (Tobin, 2023).  

MapReduce also lacks flexibility in terms of language, as it is only compatible 
with Java.  This would have presented a major hurdle in terms of using 
MapReduce for this project, which is centered around the use of Python and 
Jupyter notebooks.  The previously-described RDD's employed by Spark also 
feature superior fault tolerance when compared with MapReduce's counterpart, 
Hadoop Distributed File Systems (HDFS's) (Tobin, 2023).

Spark is known as "the Swiss army knife of big data processing" (Tobin, 2023), 
underscoring its flexibility and ease of use.  With that in mind, Spark is
clearly the ideal choice for this particular project.

*Course Material*

Apache Spark was the central focus of one of the class lectures.  The 
aforementioned shortcomings of Hadoop and Hadoop MapReduce were covered, as 
were the diverse and widespread uses and applications of Spark.  Furthermore, 
Spark is a key element of the Berkeley AMPLab Data Analytics Stack (BDAS), 
"an open source software stack that integrates software components being built 
by the AMPLab to make sense of Big Data" (BDAS Info).

*References*

BDAS Info: https://amplab.cs.berkeley.edu/software/

"The Good and the Bad of Apache Spark Big Data Processing", 2023: 
https://tinyurl.com/556rmvck

Mohan, Thyagarajan, 2023: https://tinyurl.com/d39as3cn

Pointer, 2024: https://tinyurl.com/3y9j3fse

Tobin, 2023: https://tinyurl.com/wvp5vkpp

**Docker System**

This project employed Docker Compose with a single container.  The Dockerfile
is as follows:

```plaintext
# Access the latest version of jupyter/pyspark
FROM jupyter/pyspark-notebook:latest

# Allowing browser access to notebook via localhost:8888
EXPOSE 8888

# Environment variables
ENV SPARK_HOME=/usr/local/spark

ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
ENV PATH=$SPARK_HOME/bin:$PATH

# Install py4j Python package
RUN pip install py4j

# Copy the notebook file into the container directory
COPY main_app.ipynb /home/jovyan/

# Start Jupyter server
CMD ["start-notebook.sh", "--NotebookApp.token=''"]
```

With the Dockerfile constructed as detailed above, the user is able to employ
multiple methods by which to run the code.  The user can directly access the
code output in real-time by navigating directly to the container terminal and
converting the notebook file to a single script.  Alternatively, the user can
also view and run the code directly from the notebook in his/her browser, by
navigating to localhost:8888.  I chose this approach for the purpose of
flexibility--the user can access and run the full code if he/she would like,
but has the option to simply see the output, saving time and decreasing 
procedural complexity.

The process is further streamlined by incorporating Docker Compose, using the
following .yml file:

```yml
version: '3'

services:
  jupyter:
    container_name: my_jupyter_container 
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "8888:8888"
    volumes:
      - ./main_app.ipynb:/home/jovyan/main_app.ipynb
    environment:
      - "NOTEBOOK_TOKEN="
```

With this setup, only one container needs to be run--"my_jupyter_container",
defined in the .yml file above.  

**Instructions**

*Activating the Container*

Navigate to "Spring2024_Real-time_Data_Processing_with_Apache_Spark_Streaming"
folder in terminal, then enter:

```plaintext
docker-compose up -d
```

*To access just the output*

First, access the container terminal:

```plaintext
docker exec -it my_jupyter_container /bin/bash
```

Once inside the container terminal, run these commands to access the script:

```plaintext
>jupyter nbconvert --to script main_app.ipynb
>mv main_app.txt main_app.py
```

Run the script:

```plaintext
python main_app.py
```

*To view the notebook*

Open your browser, and navigate to

```plaintext
http://localhost:8888/
```