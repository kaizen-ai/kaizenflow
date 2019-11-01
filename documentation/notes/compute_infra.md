<!--ts-->
   * [Setup research instance](#setup-research-instance)
      * [Vendor info](#vendor-info)
      * [Folders structure](#folders-structure)
      * [Home directories](#home-directories)
      * [Setup research server](#setup-research-server)



<!--te-->

# Setup research instance

## Vendor info

- Preferable we use Amazon AWS Services
- Account owner: Nikolskiy Ilya 
- Email: ilya@particle.one

## Folders structure
During the setup process we create some additional folders.

- /wd
    - Folder with users home directories.
    - Usually we use separate volume for this folder.
    - Using this approach we can guaranty that if user in some accident reason 
      fill all available space in **wd** it won't affect the system. 

- /data
    - Folder where we keep all generated data if this data should be kept on 
      local drive.
    - Usually we use separate volume for this folder.
    - Think bout this way of storing data as an intermediate storage. Preferable
      way to store the data is **S3**.



- /dump_data
    - Folder where we keep data that were dumped during some process.   
      For example:  
      We downloading a raw data from some API. We save this data in some format 
      (csv, json, MongoDB).Preferable places for any data is S3, "data" 
      directory, MongoDB backend. But to be able to save data in some format it 
      should be converted during the process. In the dump_data we save all data 
      that we get from the api without any transformation.  
       

- /http
    - Folder where we keep all data that will be shared through standard http server.

## Home directories

- root user in:
    - /home/root/
    - /home/ubuntu
    - /root
  
  Depends on system.   
  By default on AWS instance, OS type Ubuntu: root user equals to ubuntu user 
  with home directory /home/ubuntu
- all home folders for real users placed in /wd/<user_name>


## Setup research server
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
 We had a lot of changes during setup process, so we have to actualize this playbook)

```bash
$ cd infra/ansible
$ ansible-playbook playbook/init_research_server.yaml
```
