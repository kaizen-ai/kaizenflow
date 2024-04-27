#!/bin/sh -xe

# Run the config-hbase file to call on variables
. /build/config-hbase.sh

# # Update our ubuntu system
# apt-get update -y

# # Install java by specifying the java build we want
# apt-get install $minimal_apt_get_args $HBASE_BUILD_PACKAGES

# # We enter into the opt file which is where hbase is located
# cd /opt

# # Install hbase from the website, and we pipe the output of the curl 
# # into the tar function to unpack the file. And we will rename the original
# # file name to the new file name "hbase"
# curl -SL $HBASE_DIST/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz | tar -x -z && mv hbase-${HBASE_VERSION} hbase 