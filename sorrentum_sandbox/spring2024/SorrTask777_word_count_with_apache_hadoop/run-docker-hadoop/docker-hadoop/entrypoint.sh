#!/bin/bash
set -e

# Set Java and Hadoop environment variables
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64
export HADOOP_INSTALL=/usr/local/hadoop

# Update PATH to include Java and Hadoop binaries
export PATH=$JAVA_HOME/bin:$HADOOP_INSTALL/bin:$HADOOP_INSTALL/sbin:$PATH

# Verify Java executable path
echo "Java executable path: $(which java)"
echo "Java version: $(java -version)"

# Verify Hadoop installation directory
echo "Hadoop installation directory: $HADOOP_INSTALL"
echo "Hadoop version: $(hadoop version)"

# Start HDFS or YARN if needed
if [ "$1" = 'hdfs' ]; then
    $HADOOP_INSTALL/sbin/start-dfs.sh
elif [ "$1" = 'yarn' ]; then
    $HADOOP_INSTALL/sbin/start-yarn.sh
fi

# Start an interactive shell or execute the provided command
exec "$@"
