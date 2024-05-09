# Windowed Streaming Analysis with Apache Spark Streaming

## Author Information
* Author: Jacqueline Yu
* GitHub: jacyu99
* UMD email: jyu12316@umd.edu

## Description
This project uses Python within a notebook environment to orchestrate time-based aggregations, leveraging tumbling windows for granular insights into the data. Due to the fact that publicly available streaming datasets are limited, this project will use a static csv file as the data source.

## Technologies
### Apache Spark:
* Spark is a robust, open-source distributed computing framework typically used for big data workloads.
* It utilizes in-memory caching and optimized query execution for fast analytic queries, making it suitable for a wide range of use cases.
* The in-memory cache significantly speeds up machine learning algorithms that repeatedly call a function for the same set of data.
* Spark natively supports Java, Scala, R, and Python and supports code reuse across multiple workloads such as batch processing, interactive queries, real-time analytics, machine learning, and graph processing.
* The efficiency and scalability of Spark ensures that the application can handle large data volumes and complexity without compromising performance.
* In the context of this project, Spark is utilized for windowed analysis to enable high-speed data processing and analysis on a large dataset.
* Time-based aggregations for the order volume of clothing items on Amazon.in are done for the following windows:
  * Monthly
  * Weekly
  * Daily
* Time based aggregations for the percentage of orders on Amazon.in that were expedited shipping are done for the following windows:
  * Monthly
  * Weekly

### Docker:
* Docker is a platform designed to streamlline the process of building, testing, and deploying applications efficiently.
* It achieves this by encapsulating software into standardized units known as containers. Containers contain all the necessary components for the software to function seamlessly, including libraries, system tools, code, and runtime environments.
* With Docker, it's possible to simplify the deployment and scaling of applications across various environments while ensuring the reliability of code execution.
* The Dockerfile for this project sets the jupyter/pyspark-notebook image as the base image, sets the working directory, copies the current directory's contents into the container, and exposes port 8888 (default port for Jupyter Notebook) to start a Jupyter Notebook with PySpark enabled.
* Docker allows the project to be run from anyone's machine without needing to install all the software and systems used.

### Jupyter:
* Jupyter is an open-source web application that allows users to create and share documents called "notebooks" containing code, visualizations, and text. It supports numerous programming languages, including Python, R, and Scala.
* Jupyter provides an interactive computing environment that allows code to be written and executed in a cell-based format. This functionality supports iterative development and experimentation.
* Jupyter notebooks also can display various types of output such as interactive visualizations and tables, making it an excellent choice for compiling and sharing research insights.
* Jupyter is commonly used in data science and machine learning workflows because of its seamless integration with widely used libraries such as NumPy, pandas, Matplotlib, and scikit-learn. These libraries facilitate data manipulation, analysis, and modeling within Jupyter.
* A jupyter notebook is used as the IDE for this project as it provides functionality to write code, use Markdown, and output visualizations all in the same place.
