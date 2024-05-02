# Getting the hbase versions, phoneix version, and the phoenix download link
# We are also getting the specific container ID that is associated to the running
#  docker container that has hbase installed

hbase_version=2.2
p_link="http://dlcdn.apache.org/phoenix"
p_version=5.1.3
download_link=$p_link/phoenix-$p_version/phoenix-hbase-$hbase_version-$p_version-bin.tar.gz
container_id=$(sudo docker container ls -all --quiet --filter "name=hbase-docker")

#Upgrade the current system
sudo dokcer exec hbase-docker bash -c "apt -y --no-install-recommends upgrade"

# Now run the command that will have us install python inside of the container
sudo docker exec hbase-docker bash -c "apt install -y --no-install-recommends python3"

# On our local machine we will install the phoenix files
wget $download_link | tar -xzf && mv phoenix-hbase-$hbase_version-$p_version-bin phoenix

# Here we will copy both the phoenix folder, and move it into the docker container
# We also move the jar file that is required inside to the docker hbase lib directory
sudo docker cp ./phoenix/phoenix-server-hbase-$hbase_version-$p_version.jar $container_id:/opt/hbase/lib


