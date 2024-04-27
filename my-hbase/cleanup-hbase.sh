#!/bin/sh -xe

# Call on our predefined variables
. /build/config-hbase.sh

# Shows automatically installed packages and put it into a variable
AUTO_ADDED_PACKAGES=`apt-mark showauto`

# We will remove all the packages that we have installed to build the system
# and we will remove anything related to those packages
apt-get remove --purge -y $HBASE_BUILD_PACKAGES $AUTO_ADDED_PACKAGES

# Install back the java package as we will need it
apt-get install $minimal_apt_get_args $HBASE_RUN_PACKAGES

# Remove any other artifacts
rm -rf /tmp/* /var/tmp/*

apt-get clean
rm -rf /var/lib/apt/lists/*