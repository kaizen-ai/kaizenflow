Rationale for the choice of the DB

We are using a financial data obtained from the yfinance library, both PostgreSQL and MongoDB are viable options; however, PostgreSQL is a relational database management system (RDBMS) that supports structured data which the type of data that we are dealing with.

The financial data is highly structured and could require complex queries, such as joining multiple tables or performing aggregations. PostgreSQL provides robust support for SQL and has a rich set of features such as transactions, constraints, and indexes that can ensure data integrity and consistency. It is also known for its superior performance for complex queries that involve multiple tables, while MongoDB excels in read-heavy workloads and can handle large amounts of unstructured data.




Implementation and Results:

Step1:
 
Run a postgres container using the image postgres: Command is given below:
 docker run --name postgres-db -e POSTGRES_PASSWORD=docker -p 5432:5432 -d postgres

 
Step2: 
 
Run the below command :
export DOCKER_DEFAULT_PLATFORM=linux/amd64
Some environments might need this command. I needed this before spinning up the python container due to some libraries used in the python code.
 
 
Step3:
Spin up a python container with all the dependencies using the Dockerfile and docker-compose.yml file present in the app folder of the project. Navigate to the below directory and run docker-compose up command in the terminal.
Directory will be something like: “……./sorrentum/sorrentum_sandbox/projects/Issue27_Team8_Implement_sandbox_for_Yahoo_Finance/app”
 
 
Step4: Now we enter the python container using the command:
docker exec -it app-app-1 bash (accessing the terminal)
We can see all our required code is now present in this container under /code/ folder
 
 
Step5:  Now we run the code to generate a sample csv file using the download_to_csv_yahoo.py file present . Command used:
 
python3 download_to_csv_yahoo.py --start_timestamp "2023-03-19" --end_timestamp "2023-03-20" --target_dir '/code/' --interval '1m'
 

 
Step6:  We generate 6 tables (1m,5m,15m,30m, hourly, daily) in the postgres server started by the above container (‘postgres-db’). The code responsible for this task is download_db_yahoo.py . It makes use of other functions present in the other .py files in the same directory, helper directory and common directory. We pass 4 arguments while running this file. Start_timestamp, end_timestamp, interval and target_table.



Step7:
Now we update the data with latest time period using Latest_data.py file present: 
Here is an example of getting real time 1m data in the 1m table- yahoo_yfinance_spot_downloaded_1min


