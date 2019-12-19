<!--ts-->
   * [Set up research instance](#set-up-research-instance)
      * [Vendor info](#vendor-info)
      * [Folders structure](#folders-structure)
      * [Home directories](#home-directories)
      * [Set up research server](#set-up-research-server)
      * [Mount new volumes](#mount-new-volumes)



<!--te-->

# Set up research instance

## Vendor info

- We prefer to use Amazon AWS Services
- Account owner: Ilya Nikolskiy
- Email: ilya@particle.one

## Folders structure

- During the setup process of a server we create the following additional
  folders.

- `/wd`
  - Folder with user home directories.
  - Usually we use separate volume for this folder.
  - Using this approach we can guarantee that if a user accidentally fills all
    available space in **wd**, then it won't affect the system.

- `/data`
  - Folder where we keep all generated data if this data should be kept on local
    drive.
  - Usually we use a separate volume for this folder.
  - Think about this way of storing data as intermediate storage. The preferred
    way to store data is **S3**.

- `/dump_data`
  - Folder where we keep data that were dumped during some process. For example:
    We are downloading raw data from some API. We save this data in some format
    (csv, json, MongoDB). Preferable places for data is S3, "data" directory,
    MongoDB backend. But to be able to save data in some format it should be
    converted during the process. In the dump_data we save all data that we get
    from the api without any transformation.

- `/http`
  - Folder where we keep all data that will be shared through standard http
    server.

## Home directories

- Root user in:
  - /home/root/
  - /home/ubuntu
  - /root

  System-dependent. By default on AWS instance, OS type Ubuntu: root user equals
  ubuntu user with home directory /home/ubuntu

- All home folders for real users placed in /wd/<user_name>

## Set up research server

- We use ansible script for the setup

- Hosts.txt file:
  ```bash
  infra/ansible/hosts.txt
  ```
- Research servers section:

  ```bash
  [research_servers]
  ```

- Run playbook: (WARNING: at the moment [2019-10-17] roles not in the actual
  state. We had a lot of changes during setup process, so we have to actualize
  this playbook)
  ```bash
  $ cd infra/ansible
  $ ansible-playbook playbook/init_research_server.yaml
  ```

## Mount new volumes

- To mount new volumes in the system we use
  [this instruction](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-using-volumes.html)

- In common case next commands will be enough:

  ```bash
  [ec2-user ~]$ lsblk
  NAME    MAJ:MIN RM  SIZE RO TYPE MOUNTPOINT
  xvda    202:0    0    8G  0 disk
  -xvda1  202:1    0    8G  0 part /
  xvdf    202:80   0   10G  0 disk

  [ec2-user ~]$ sudo mkfs -t xfs /dev/xvdf

  [ec2-user ~]$ sudo mkdir /data

  [ec2-user ~]$ sudo mount /dev/xvdf /data
  ```

- To add volume in `fstab` use following commands

  ```bash
  [ec2-user ~]$ sudo blkid
  /dev/xvda1: LABEL="/" UUID="ca774df7-756d-4261-a3f1-76038323e572" TYPE="xfs" PARTLABEL="Linux" PARTUUID="02dcd367-e87c-4f2e-9a72-a3cf8f299c10"
  /dev/xvdf: UUID="aebf131c-6957-451e-8d34-ec978d9581ae" TYPE="xfs"
  [ec2-user ~]$ sudo vim /etc/fstab
  ```

- `fstab` row examples:
  ```bash
  LABEL=cloudimg-rootfs   /        ext4   defaults,discard        0 0
  UUID=763d28b5-1758-49da-be21-6c01d06e9d94       /wd     xfs defaults,nofail     0       2
  UUID=9ede0137-4a38-4501-a1b8-ca1e1fb32001       /data   xfs defaults,nofail     0       2
  UUID=f25e784e-2941-4f2c-9047-3d5ec07378cb       /dump_data      xfs defaults,nofail     0       2
  UUID=59bc292c-8b46-4a27-96f6-b7f6d3ef667f       /s3     xfs     defaults,nofail 0       2
  default00-bucket /s3/default00-bucket fuse.s3fs _netdev,allow_other,passwd_file=/etc/passwd-s3fs-default00-bucket 0 0
  ```
