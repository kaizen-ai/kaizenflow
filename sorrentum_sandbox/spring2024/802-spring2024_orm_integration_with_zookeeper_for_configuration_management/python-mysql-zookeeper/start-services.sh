#!/bin/bash

# 启动MySQL服务
echo "Starting MySQL..."
mysqld --user=mysql --console &

# 等待MySQL服务就绪
echo "Waiting for MySQL to be ready..."
while ! mysqladmin --user=root --password=$MYSQL_ROOT_PASSWORD --host="localhost" ping --silent; do
    sleep 1
done
echo "MySQL is ready."

# 启动Zookeeper
echo "Starting Zookeeper..."
/usr/share/zookeeper/bin/zkServer.sh start-foreground
