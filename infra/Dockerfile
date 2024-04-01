# Use the official Airflow image as a parent image
FROM apache/airflow:2.7.1

# Switch to the airflow user
USER airflow

# Install your required Python packages
RUN pip install python-telegram-bot apache-airflow-providers-telegram
