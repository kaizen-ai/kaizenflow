# HomeWork_v1

## 1、description

ORM Integration with ZooKeeper for Configuration Management
Difficulty: 2

Technology: SQLAlchemy, ZooKeeper

Description: Integrate SQLAlchemy ORM with ZooKeeper for configuration management. Store application configurations in ZooKeeper nodes, and develop scripts to read and update configuration values using SQLAlchemy ORM for data manipulation. This project showcases the integration of ORM with ZooKeeper for centralized configuration management in distributed systems.

https://medium.datadriveninvestor.com/building-a-distributed-config-server-using-zookeeper-6570363799e5

## 2、Environment

Docker desktop+python 3 with virtual environment + zookeeper +SQLAlchemy+MySql 8.0.37 + kazoo

More details in the picture：

![image-20240504195741480](C:\Users\86156\AppData\Roaming\Typora\typora-user-images\image-20240504195741480.png)

## 3、My implementation method

```
1、I used docker to create an image, which used the MySQL+Debian base image. On this basis, I installed Zookeeper and Python3. In order to complete the project requirements, I also installed Kazoo, SQLAlchemy and other third-party libraries.
2、In order to realize the requirements of the topic: use Zookeeper, ORM and use Python to change the configuration file, I wrote two Python scripts, as shown in the figure below
```

![image-20240504200218228](C:\Users\86156\AppData\Roaming\Typora\typora-user-images\image-20240504200218228.png)

```
1、The Zookeeper_Config.py file is to create the Zookeeper root node and upload the basic connection to the database to facilitate subsequent operations. The zookeeper_oper.py is to change the configuration script and save it to the local database through the python orm connection, completing the requirements of the project.
```

## 4、	Run my code

```
I packaged my Docker image and you can run my script by executing the following command：
1、bash                                          #this command for change into the bash mode
2、source myenv/bin/activate					  #this command for activate my virtual environment
3、python3 zookeeper_oper.py 				  #this command for run my script


if you want to check my script run this command(i will upload all my code in github)
1、nano zookeeper_oper.py
```