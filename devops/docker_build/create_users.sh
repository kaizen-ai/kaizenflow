# This needs to be kept in sync with devops/docker_build/etc_sudoers.
# TODO(gp): Do a loop instead of copy-paste.
# TODO(gp): Find a solution where you don't have to specify all the potential
# users up front.
# The problem is that /etc/sudoers needs to be ready before the entrypoint is
# executed. On the other side we get to know the user ID when the entrypoint
# is executed. Thus we can't easily use a script to generate this script and
# `etc_sudoers` but we need to do it when the Docker container is created.
OPTS="--home-dir /home"

# Mac user.
useradd -u 501 $OPTS user_501
usermod -aG docker user_501

# Linux users.
useradd -u 1001 $OPTS user_1001
usermod -aG docker user_1001

useradd -u 1002 $OPTS user_1002
usermod -aG docker user_1002

useradd -u 1003 $OPTS user_1003
usermod -aG docker user_1003

useradd -u 1004 $OPTS user_1004
usermod -aG docker user_1004

useradd -u 1005 $OPTS user_1005
usermod -aG docker user_1005

useradd -u 1006 $OPTS user_1006
usermod -aG docker user_1006

useradd -u 1007 $OPTS user_1007
usermod -aG docker user_1007

useradd -u 1008 $OPTS user_1008
usermod -aG docker user_1008

useradd -u 1009 $OPTS user_1009
usermod -aG docker user_1009

useradd -u 1010 $OPTS user_1010
usermod -aG docker user_1010

sudo chmod -R 777 /home

# Allow users to access /mnt/tmpfs.
# TODO(gp): We could change the permissions in fstab.
sudo chmod 777 /mnt
