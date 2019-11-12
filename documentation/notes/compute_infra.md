<!--ts-->
   * [Setup research instance](#setup-research-instance)
      * [Vendor info](#vendor-info)
      * [Folders structure](#folders-structure)
      * [Home directories](#home-directories)
      * [Setup research server](#setup-research-server)



<!--te-->

# Set up research instance

## Vendor info

- Preferably we use Amazon AWS Services
- Account owner: Nikolskiy Ilya 
- Email: ilya@particle.one

## Folders structure
During the setup process we create the following additional folders.

- /wd
    - Folder with user home directories.
    - Usually we use separate volume for this folder.
    - Using this approach we can guarantee that if a user accidentally
      fills all available space in **wd**, then it won't affect the system.

- /data
    - Folder where we keep all generated data if this data should be kept on 
      local drive.
    - Usually we use a separate volume for this folder.
    - Think about this way of storing data as intermediate storage. The
      preferred way to store data is **S3**.

- /dump_data
    - Folder where we keep data that were dumped during some process.   
      For example:  
      We are downloading raw data from some API. We save this data in some
      format (csv, json, MongoDB).  Preferable places for data is S3, "data"
      directory, MongoDB backend. But to be able to save data in some format it
      should be converted during the process. In the dump_data we save all data
      that we get from the api without any transformation.

- /http
    - Folder where we keep all data that will be shared through standard http
      server.

## Home directories

- root user in:
    - /home/root/
    - /home/ubuntu
    - /root
  
  System-dependent.
  By default on AWS instance, OS type Ubuntu: root user equals ubuntu user 
  with home directory /home/ubuntu

- all home folders for real users placed in /wd/<user_name>


## Set up research server
- We use ansible script for the setup

- hosts.txt file:
```bash
infra/ansible/hosts.txt
```
- research servers section:
```bash
[research_servers]
```

- run playbook:  
  (WARNING: at the moment [2019-10-17] roles not in the actual state.   
  We had a lot of changes during setup process, so we have to actualize this
  playbook)

```bash
$ cd infra/ansible
$ ansible-playbook playbook/init_research_server.yaml
```
