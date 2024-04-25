# Use Ubuntu as the base image
FROM ubuntu:latest

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64

# Install Java, wget, and other dependencies
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk wget && \
    rm -rf /var/lib/apt/lists/*

# Set Hadoop environment variables
ENV HADOOP_VERSION 2.3.0
ENV HADOOP_INSTALL /usr/local/hadoop
ENV HADOOP_MAPRED_HOME $HADOOP_INSTALL
ENV HADOOP_COMMON_HOME $HADOOP_INSTALL
ENV HADOOP_HDFS_HOME $HADOOP_INSTALL
ENV HADOOP_COMMON_LIB_NATIVE_DIR $HADOOP_INSTALL/lib/native
ENV YARN_HOME $HADOOP_INSTALL
ENV HADOOP_CONF_DIR $HADOOP_INSTALL/etc/hadoop

# Installing Hadoop
RUN wget http://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -zxf hadoop-$HADOOP_VERSION.tar.gz && \
    rm hadoop-$HADOOP_VERSION.tar.gz && \
    mv hadoop-$HADOOP_VERSION $HADOOP_INSTALL && \
    mkdir -p $HADOOP_INSTALL/logs

# Creating symlink for Hadoop configuration files
VOLUME /data

# Copying Hadoop configuration files from local directory
COPY core-site.xml $HADOOP_CONF_DIR/core-site.xml
COPY yarn-site.xml $HADOOP_CONF_DIR/yarn-site.xml
COPY mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml
COPY hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml

# Copy the entrypoint script into the image
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Set the entry point to the custom entrypoint script
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
