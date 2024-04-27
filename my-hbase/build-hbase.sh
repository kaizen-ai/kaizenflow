#!/bin/sh -xe

# We will run the config-hbase.sh in order to call on our 
# predefined variables
. /build/config-hbase.sh

# We will set a variable to the working directory in the container
here=$(pwd)

# Delete all uneeded files from the hbase files 
rm -rf docs *.txt LEGAL
rm -f */*.cmd

# Put JAVA_HOME into hbase-env.sh
sed -i "s,^. export JAVA_HOME.*,export JAVA_HOME=$JAVA_HOME," conf/hbase-env.sh

# Set interactive shell defaults
cat > /etc/profile.d/defaults.sh << EOF
JAVA_HOME=$JAVA_HOME
export JAVA_HOME
EOF

cd /usr/bin
ln -sf $here/bin/* .
