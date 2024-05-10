# Apache Kafka to create a data streaming platform

## Author info

- Author: Gabriel Oladipupo Lawson
- GitHub account: DIPOLAWSON
- UMD email: OLAWSON1@UMD.EDU
- Personal email: LAWSONOLADIPUPO200@GMAIL.COM

## Description

The project involves setting up Apache Kafka and a PostgreSQL database encapsulated in a single or multiple Docker containers with docker-compose to establish a streamlined data streaming platform. Utilize Python to fetch data from an external source, format it for Kafka ingestion (Topics), and configure producers for efficient data transfer into Kafka topics. Python-based Kafka consumers will perform some EDA using Jupyter notebook, process and validate the data before storing it into PostgreSQL, utilizing a predefined schema. The goal is to create a reliable system that seamlessly downloads, processes, and securely stores external data in real-time using Kafka as the intermediary, Python for logic handling, and Docker for deployment flexibility.

## Technologies

## Zookeeper
As a centralized service, Zookeeper handles configuration data management, naming, distributed synchronization, and group service support. 
Distributed systems depend on the preservation of a "source of truth" regarding the setup and state of various services inside the network. 
In environments where several services necessitate consistent and reliable coordination, Zookeeper makes sure that all involved nodes are in sync with one another, preventing discrepancies in service statuses and configurations.

## Key Configurations
#### ZOOKEEPER_CLIENT_PORT
The port on which Zookeeper waits for client connections is indicated by this variable. 
It is necessary to enable communication between Zookeeper and client apps, such as Kafka.

#### ZOOKEEPER_TICK_TIME
The basic time unit in Zookeeper is the duration of a single tick, which is expressed in milliseconds. 
Processes like heartbeat and timeouts, which are essential for preserving the functionality and overall health of the Zookeeper service, are impacted by this parameter.

## Kafka
### Kafka's Setup Linked with Zookeeper
Apache Kafka uses Zookeeper to maintain cluster configurations and states. 
To ensure fault tolerance and data consistency throughout the cluster, Kafka brokers use Zookeeper to manage leader election of partitions and keep track of which brokers are active.
### Key Configurations
#### KAFKA_BROKER_ID
A cluster's unique identification for every Kafka broker.
The ability of the Kafka system to recognize and control various brokers is essential, particularly for scaling up or down. 
#### KAFKA_ZOOKEEPER_CONNECT
Indicates the hostname and port of the Zookeeper service as part of the connection string for Zookeeper. 
Since Zookeeper is used by Kafka to maintain its internal state, this connection is essential to its proper operation. 
#### KAFKA_ADVERTISED_LISTENERS
Sets up the address that producers and customers will see the broker promote. 
This configuration is essential for client connections as well as network communication within the Kafka cluster. 
#### KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
Specifies the protocols that listeners utilize. 
This information is crucial for setting up security and communication protocol standards. 

## PostgreSQL
### Usage of PostgreSQL
A relational database called PostgreSQL is used to efficiently store and handle project data. 
It is the best option for managing massive amounts of data with high transaction rates since it provides strong data integrity and support for sophisticated queries. 
### Configuration Details 
#### Ports
Applications from outside can connect to the database by mapping external port 5320 to internal PostgreSQL port 5432. 
#### Volumes
When data persistence is used with volumes, information is preserved through container restarts and removals. 
#### Configuration
Configuring the default user, password, and database name in the environment's POSTGRES_USER, POSTGRES_PASSWORD, and POSTGRES_DB settings guarantees that the application has the right authorization to safely communicate with the database. 



## Python-App
### Configuration for Multiple Modes
The Python application container may switch between a conventional web application and a Jupyter Notebook environment thanks to its special configuration.
Because of this flexibility, developers, and analysts can run the application in a production-like environment or build and analyze in a Jupyter Notebook, depending on what suits their needs at the moment.
#### APP_MODE
This environment variable is very important since it controls whether the container runs the web application or launches a Jupyter server. 
The container's usefulness is increased in this configuration, meeting requirements for both development and operations.
#### Dependencies
The Python program depends on PostgreSQL for data storage and Kafka for data streaming.
This guarantees that every component of the data pipeline is linked and ready to go before the application launches. 

## Docker: Containerization
Docker is a comprehensive platform that makes leveraging containerization technologies to create, ship, and execute applications easier.
With the help of containers, software developers can bundle a program together with all of its dependencies into a standardized unit that will guarantee consistent operation across all environments.
Docker's functionality is expanded with the help of Docker Compose, a tool for creating and managing multi-container Docker applications. With Docker Compose, you can use a YAML configuration file to define the services, networks, and volumes, and you can use a single command to configure, run, and stop all the components of a complicated application.
Docker is a containerization platform that allows developers to package applications and their dependencies into lightweight, portable containers.
-  Docker containers provide a high level of isolation, allowing applications to run independently of the underlying host system.
-  Docker simplifies dependency management by allowing developers to specify the exact environment needed for their applications. This reduces compatibility issues and ensures that applications run consistently across different development machines and servers.
-   Docker encourages a modular approach to application development, where applications are broken down into smaller, independent services running in separate containers. This facilitates the adoption of microservices architecture, allowing for easier maintenance, updates, and scaling of individual components.

## Docker System Overview

### Components of the Docker System

- Server Container:
</t><br>•	PostgreSQL: Used for storing and managing your relational database.
</t></br>•	Kafka: A distributed streaming platform used for building real-time data pipelines and streaming applications.
</t></br>•	Zookeeper: Coordinates distributed brokers in Kafka to maintain a shared state between them.

- Client Container:
</t><br>• Jupyter Notebook provides an interactive environment where you can execute code and query databases like PostgreSQL using libraries such as psycopg2.



### Dockerfiles:
- Dockerfile_server and Dockerfile_client:
  
    - This Dockerfile defines the environment and instructions for building the server/client container.
    - It starts with the ZOOKEEPER AND KAFKA for .
    - Copies the server/client.py file into the container's filesystem.
    - Installs the pyzmq library using pip to enable ZeroMQ functionality.  
    - Sets the command to execute when the container starts, which is to run the server/client.py script.

- docker-compose.yaml:

    - It specifies three services: server, client, and nginx.
    - Each service is built using its respective Dockerfile (Dockerfile_server, Dockerfile_client, and Dockerfile_nginx).
    - The server service is connected to the zmq_network bridge network, ensuring communication with other services.
    - The client service depends on the server service, ensuring that the server is started before the client.
    - The nginx service is configured to listen on port 80 and forward requests to the backend servers.
    - All services are connected to the zmq_network bridge network for communication between containers.

![image]https://github.com/kaizen-ai/kaizenflow/commit/aa81d17d1ccbcab563935f29a90f547c6f672786)


### Example Output
- On running the docker containers we may observe an output that looks like:
```
gabriel@DIPO:~/605-FINAL-PROJECT$ docker compose up -d
WARN[0000] /home/gabriel/605-FINAL-PROJECT/docker-compose.yml: `version` is obsolete
[+] Running 4/0
 ✔ Container 605-final-project-postgres-1    Running                                                                            0.0s
 ✔ Container 605-final-project-zookeeper-1   Running                                                                            0.0s
 ✔ Container 605-final-project-kafka-1       Running                                                                            0.0s
 ✔ Container 605-final-project-python-app-1  Running                                                                            0.0s
```

## Mock Workflow of the docker system
The smooth operation of Zookeeper, Kafka, PostgreSQL, and the Python program in a Dockerized setup creates a workflow that is necessary for efficient data processing and analysis. The connectivity of these services is orchestrated by Docker Compose, which makes sure that the right services are created in the right sequence with the required configurations for inter-service communication.

### Data Flow and Processing
#### From Kafka to PostgreSQL
</t></br>• Data Ingestion: Kafka serves as the gateway for real-time data streams, and it is through this that data is initially ingested into the system. Kafka's job is to buffer incoming data, handle it effectively, and make it available for processing. Maintaining performance while working with high-volume or high-velocity data requires doing this. 
</t></br>• Data processing: The Python program, operating in its Docker container, serves as a consumer of data once it is available in Kafka. To retrieve fresh data entries, it subscribes to the pertinent Kafka topics.After that, the data is processed; depending on the logic of the application and the type of data, this processing may involve transformations, aggregations, or enrichments. 


</t></br>• Data Storage: PostgreSQL is used to store the data after it has been processed. In this step, inserting or updating data in the database is often done by the Python application using SQL commands. The permanent storage layer is provided by PostgreSQL, which guarantees data integrity and supports sophisticated queries that may be required for additional research or reporting.
#### Interaction with Jupyter Notebook
</t></br>•	Data Visualization and Analysis: An interactive interface for data analysis and visualization is provided by the Jupyter Notebook, which is accessed through the Python program. In order to retrieve data straight from the database, it connects to PostgreSQL using common database connectors that are available in Python (such as psycopg2 or SQLalchemy).

</t></br>•	Real-time Data Access: To obtain real-time data streams for analysis, the notebook can also communicate directly with Kafka. This feature is especially helpful for tracking and responding to real-time data, offering insights as they become available.

</t></br>•	Visual Output: Graphs, charts, and maps that make the data comprehensible and useful can be created in the notebook by using visualization libraries such as matplotlib, seaborn, or Pandas. For raw data to be swiftly transformed into shareable or actionable visual insights, this integration is essential. 
Workflow Benefits
</t></br>•	Efficiency: By enabling independent scaling of each component in response to load, the Dockerized system guarantees effective resource utilization. 
</t></br>•	Flexibility: Without having to reconfigure or redeploy the services, analysts can go between exploring data in Jupyter and running the application in a production-like environment.
</t></br>•	Reproducibility: Applying Docker guarantees a consistent environment for processing and analyzing data, which is essential for repeatable research and development procedures. 


### API Integration
#### Introduction to APIs and Python
APIs, or application programming interfaces, are essential to contemporary software development since they serve as the link between various services or components of the product. 
By specifying communication channels, they let systems to communicate with one another and, generally, facilitate the retrieval and manipulation of data across various software environments.
Developing scalable, modular, and effective software systems requires this interoperability.


#### Using Python Libraries to Fetch and Process API Data
The requests package in Python is frequently used to send various types of HTTP requests. It is easy to use and supports a wide range of HTTP request types, such as GET, POST, PUT, DELETE, and so on, all of which are necessary for communicating with RESTful APIs. 
However, pandas is a very helpful tool for working with and analyzing data in Python, especially when working with big datasets or intricate data manipulations.

### THE API USED IN MY PROJECT
I made use of WeatherAPI.com, which offers a wealth of information relevant to weather conditions. In addition to specific weather variables like temperature, humidity, wind speed, and direction, this API provides real-time weather information, historical weather data, and forecasts for up to 15 days. It can be used for a wide range of tasks, from basic weather updates to intricate data analysis for planning agricultural operations and managing disasters.

Since the data is delivered by the API in JSON format, integrating and modifying it in your Python applications is simple. In addition to hourly and daily predictions that include specific elements like precipitation, cloudiness, visibility, and air pressure, you may obtain actual weather data. In projects like yours, where comprehending weather patterns might be vital, this flexibility and depth of data enable comprehensive analysis and visualization. 

I can effectively retrieve and process this precise weather data by integrating this API with your Python project. Python's ability to work with JSON data and its strong tools (such as matplotlib for charting and pandas for data manipulation) allow for efficient data visualization and analysis right out of the API answers. 



## LINKING THE POSTGRESQL TO MY JUPYTER NOTEBOOK THEN CREATING A TABLE QUERY

Database Connection Establishment: First, the code uses the host, port, database name, user, and password to connect to a PostgreSQL database. Setting up a session with the database server in this stage is essential since it enables further actions like querying or changing the database.

Cursor Creation: The connection is used to build a cursor object. This cursor is used to do database operations in Python. It serves as a go-between for Python and the PostgreSQL CLI, allowing Python code to execute SQL statements.

Creating a Table: If the weather data table does not already exist, the code provides an SQL query to create it. The purpose of this table is to hold the names of the cities and the accompanying meteorological information, such as wind speed and temperature. Re-creating an existing table can lead to mistakes and data loss, thus the “CREATE TABLE IF NOT EXISTS” line makes that the table is only created if it does not already exist.

Inserting Data: A loop in the code iterates through a collection of meteorological data that was probably previously gathered from an API. The city name, temperature, and wind speed are extracted from each list element, and an INSERT SQL query is created to add these details to the weather data table. Every insertion is a transaction that is started with the SQL command and data tuple formatting and is finished with the cursor.

Transaction Commitment: The transactions are committed following the execution of the SQL statements to establish the table and enter data. The modifications are now complete and will remain in the database after this action. To make sure that the activities don't only remain pending, transactions must be committed.

Resource Cleanup: Lastly, the database connection and the cursor are shut down. Since server resources are limited and might create a bottleneck if connections are left open forever, closing the connection is a crucial step in database management. 


## PUTTING THE CITIES, TEMPERATURE AND WIND SPEED DATA IN THE POSTGRESQL TABLE

Database Connection and Cursor Creation: In both scripts, connecting to a PostgreSQL database using the psycopg2 package is the initial step. They supply details such database name, user, password, host, and port in order to establish a connection to the right database on the assigned server. Once the connection is established, a cursor object is generated. This cursor is used to execute SQL statements using Python.
Checking Table Existence and Data Retrieval: A preliminary check is made in the first script to see if the database has the particular table ('weather_data'). This is accomplished by running a SELECT query over the information_schema.tables to see if the 'weather_data' table is there. The script alerts the user if the table doesn't exist and then retrieves data from it if it does. When actions on a table depend on the table's existence, this type of check is essential to preventing mistakes.
Data Fetching and DataFrame Conversion: An SQL query is run to retrieve temperature and wind speed data from the 'weather_data' table as soon as it is verified that the table exists or is specifically mentioned in the second script. Using the fetchall() method, the data retrieved by the cursor is fetched all at once and loaded into a pandas DataFrame. The column names in this DataFrame are named specifically for easier reading and manipulation. Pandas is a recommended option for managing structured data since it offers a practical and strong data structure for data analysis and manipulation.
Closing Resources: In both scripts, the cursor and the database connection are closed after the required SQL operations have been carried out and the data has been handled. In order to release database resources—which can be scarce on a server and must be carefully managed to prevent performance problems—the database connection and cursor must be closed properly.

Error Handling: To handle any exceptions that may arise during database operations, such as connection failures or SQL issues, the first script has a try-except block. This is a recommended programming strategy that increases the application's resilience by offering a gentle way to handle unforeseen problems. In order to prevent database connection leaks, it is essential to use finally to guarantee that the database connection is closed even in the event of an error.

```
import psycopg2
import pandas as pd

def fetch_weather_data():
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="postgres", 
        user="postgres", 
        password="mypassword", 
        host="localhost", 
        port="5432"
    )
    cur = conn.cursor()

    # Execute query to retrieve temperature and wind_speed
    cur.execute("SELECT temperature, wind_speed FROM weather_data")
    data = cur.fetchall()
    
    # Close the connection
    cur.close()
    conn.close()

    # Load data into a DataFrame
    df = pd.DataFrame(data, columns=['temperature', 'wind_speed'])
    return df

# Fetch data
df = fetch_weather_data()
print(df.head())
```
## Performing Linear Regression on the Data and analyzing the results.
The basic predictive modeling approaches are then demonstrated by using this data to build a linear regression model that predicts temperature depending on wind speed. When one wishes to comprehend or forecast continuous variables based on other variables, linear regression is a useful tool. Additionally, the script has a visualization component that helps visualize the link between temperature and wind speed by plotting the actual data points against the anticipated regression line.
The results from the linear regression, as noted in your provided output, include an intercept and a coefficient for wind speed. Here’s a detailed explanation of these results:
</t></br>• Intercept (290.23534597869286): This value represents the predicted temperature (in Kelvin) when the wind speed is zero. Essentially, it's the starting point of the regression line on the y-axis. In the context of this weather dataset, you could interpret it as the baseline temperature that the model predicts in the absence of wind.
</t></br>• Coefficient for Wind Speed (0.5418079806625375): This coefficient tells us how much the temperature is expected to increase with each one-meter-per-second increase in wind speed. Specifically, for each 1 m/s increase in wind speed, the temperature is predicted to increase by about 0.542 Kelvin. This relationship is key to understanding how wind speed influences temperature, according to the model.
### These components form the linear regression equation:
</t></br>• Temperature=290.235+0.542×Wind SpeedTemperature=290.235+0.542×Wind Speed
This equation can be used to predict the temperature based on given values of wind speed.
Fetching Data and Handling Results
The query tries to fetch a single record that matches the city name. If a matching record is found, it is returned; otherwise, the script indicates that no record was found for that city

```
import psycopg2
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt

def fetch_weather_data():
    try:
        conn = psycopg2.connect(
            dbname="postgres", 
            user="postgres", 
            password="mypassword", 
            host="localhost", 
            port="5432"
        )
        cur = conn.cursor()
        cur.execute("SELECT temperature, wind_speed FROM weather_data")
        data = cur.fetchall()
        cur.close()
        conn.close()
        return pd.DataFrame(data, columns=['temperature', 'wind_speed'])
    except psycopg2.Error as e:
        print("Error retrieving data from PostgreSQL:", e)
        return pd.DataFrame()

def plot_regression(X, y, predicted_y):
    plt.scatter(X, y, color='blue', label='Actual data', alpha=0.5)
    plt.plot(X, predicted_y, color='red', label='Regression line')
    plt.title('Temperature vs Wind Speed')
    plt.xlabel('Wind Speed (m/s)')
    plt.ylabel('Temperature (K)')
    plt.legend()
    plt.grid(True)
    plt.show()

def main():
    df = fetch_weather_data()
    if not df.empty:
        X = df[['wind_speed']]  # scikit-learn expects 2D array for X
        y = df['temperature']
        model = LinearRegression()
        model.fit(X, y)
        print(f'Intercept: {model.intercept_}')
        print(f'Coefficient for wind speed: {model.coef_[0]}')
        predicted_temps = model.predict(X)
        plot_regression(df['wind_speed'], df['temperature'], predicted_temps)

if __name__ == "__main__":
    main()
```

## Conclusion
The project showcases the versatility and efficiency of Kafka, PostgreSQL and Jupyter Notebook in building distributed and scalable systems. By incorporating features such as error handling, and asynchronous communication, we have created a robust client-server architecture suitable for various real-world applications.

Demo link: [https://drive.google.com/file/d/17nlUTElRDGIZmdLY7SWDoJafAB4mtwTa/view?usp=sharing](https://drive.google.com/file/d/1MXGzvrtCmJ7notjt_WJBcSRBUsJ1inCN/view?usp=drive_link)

