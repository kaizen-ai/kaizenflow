#!/bin/sh -xe

# This file is made to predefined all other sh files

# Link the site in which the hbase file will be downloaded from;
# we will give this the variable name HBASE_DIST
HBASE_DIST="http:/dlcdn.apache.org/hbase"

# Now we will prevent some stuff to be ran 
export INITRD=no
export DEBIAN_FRONTEND=noninteractive

# We will export the java 11
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# This line is used to limit apt get, the no install recommendations will tell 
# apt to not install recommended packages along with the desired package. 
# We don't want any more clutter in the system.
# We will set tehse commands to a varibl 
minimal_apt_get_args='-y --no-install-recommends'


# We will build the packages using the curl commands
HBASE_BUILD_PACKAGES="curl"

# We will run the packages using the java commands
HBASE_RUN_PACKAGES="openjdk-11-jre-headless"

