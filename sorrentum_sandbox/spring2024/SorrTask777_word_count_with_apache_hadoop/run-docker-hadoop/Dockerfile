FROM docker-hadoop

# Set Java home environment variable to the correct path
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64

# Update Hadoop configuration to use JAVA_HOME
RUN sed -i "s|^export JAVA_HOME=.*$|export JAVA_HOME=$JAVA_HOME|g" /usr/local/hadoop/etc/hadoop/hadoop-env.sh
