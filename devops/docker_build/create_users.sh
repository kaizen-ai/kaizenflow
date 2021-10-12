# TODO(gp): Do a loop instead of copy-paste.
# TODO(gp): Find a solution where you don't have to specify all the potential
# users up front.
useradd -u 1001 --no-create-home user_1001
usermod -aG docker user_1001

useradd -u 1002 --no-create-home user_1002
usermod -aG docker user_1002

useradd -u 1003 --no-create-home user_1003
usermod -aG docker user_1003

useradd -u 1004 --no-create-home user_1004
usermod -aG docker user_1004
