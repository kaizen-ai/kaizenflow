# TODO(gp): Do a loop instead of copy-paste.
# TODO(gp): Find a solution where you don't have to specify all the potential
# users up front.
OPTS="--home-dir /home"
useradd -u 1001 $OPTS user_1001
usermod -aG docker user_1001

useradd -u 1002 $OPTS user_1002
usermod -aG docker user_1002

useradd -u 1003 $OPTS user_1003
usermod -aG docker user_1003

useradd -u 1004 $OPTS user_1004
usermod -aG docker user_1004

sudo chmod -R 777 /home

# Allow users to access /mnt/tmpfs.
# TODO(gp): We could change the permissions in fstab.
sudo chmod 777 /mnt

# Allow users to access /tmp.cache.disk.
mkdir /tmp.cache.disk
sudo chmod 777 /tmp.cache.disk
