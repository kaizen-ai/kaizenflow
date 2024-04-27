FROM ubuntu:bionic

COPY *.sh /build/

ENV HBASE_VERSION 2.5.8

RUN /build/prepare-hbase.sh  
#     # Go into the hbase file, and run the build-hbase.sh file
#     cd /opt/hbase && /build/build-hbase.sh \
#     # Go back to the home directory, run the cleanup-hbase.sh, and remove the
#     # buld folder so that we have a clean system
#     cd / && /build/cleanup-hbase.sh && rm -rf /buld

# VOLUME /data

# # From our own files, we are going to add the .xml file into the folder that hbase
# # is located, inside of the container 
# ADD ./hbase-site.xml /opt/hbase/conf/hbase-site.xml


