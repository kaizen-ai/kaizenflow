**Purpose Of This File**
---------------------------

  This repository seeks to show the advantages of using Secondary Indexing in HBase. In Kaizenflow, this is
  under the branch SorrTask800. 

  Hbase is a columnar database that utilizes Hadoop as the foundation. However, for simplicity, this installation will not
be deploying Hadoop clusters, and thus we will have Hbase manage Zookeeper.


**Running Our Program**
---
Make sure to follow Installation file in order to get everything installed. Once you have your
system set up, we will go into hbase/bin and run the start-hbase.sh file by running

./start-hbase.sh


This will start Hbase. 


To start the Phoenix shell; go into your Phoenix file, and run the sqlline.py file

**Loading In Data**
---
In order to see the affects, we need to load in the data.

Use the txt files for Hbase data. To load in the data, make sure you are in hbase/bin, and run the following command

  - hbase shell data.txt
  
  - hbase shell data2.txt


For the phoenix data, before loading the data, you must run the phoenix shell first and make the table. 
So open Phoenix shell and run the following command

CREATE TABLE data (
  my_pk CHAR(20) not null,
  FIRST_NAME CHAR(50),
  LAST_NAME CHAR(50), 
  AGE INTEGER,
  ADDRESS CHAR(50)
  CONSTRAINT pk PRIMARY_KEY (my_pk));


After you are done creating the table, exit the phoenix shell using ctrl-z. 

To load in data, run the following commands

  - ./psql.py -t DATA data.csv
  - ./psql.py -t DATA data2.csv


**Testing Secondary Indexing**
---
Open the hbase shell, and run a full scan using the command
  -scan 'users'

The time here should be around .4s. Exit the hbase shell.

Open the phoenix shell, and run a simple select all query, by running the command
  - SELECT * FROM data

The time for this execution is ~.1s or even faster. 

Phoenix, in its general queries, will automatically implement secondary indexing for its queries.
We can see that when we use Phoenix for queries, we are getting more efficent speeds. Take note 
that the amount of data here is only 300 with 4 different attributes to each data. 

Pictoral results are shown in the Results file
  
