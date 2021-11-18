# Ansible Infrastructure Documentation
## Running ansible playbooks - basic introduction
#### Installing ansible

See official documentation located here:
```sh
https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html
```
#### Download code repository
Use git clone to download and setup local git repository for ansible playbooks for the first time. Otherwise use git pull to download latest version of ansible playbooks
```sh
git clone git@github.com:cryptokaizen/cmamp.git
```
#### Running ansible playbook
Inside the project folder, navigate to the playbooks folder.
```sh
cd infra/ansible/
```
Folders plays and roles are the important ones.
Example of running a playbook:
```sh
ansible-playbook plays/server_configure_os.yml --user ubuntu --private-key=/home/your_user/id_rsa -i 10.25.34.55, -e "disk_configuration_enabled=false"
```
| Parameter Example | Explanation |
| ------ | ------ |
| plays/plays/server_configure_os.yml | Which playbook to apply on remote host |
| --user your_user | Username to use - needs to have admin level credentials |
| --private-key=/home/your_user/id_rsa | Path to the private key associated with username |
| -i 10.25.34.55, | Destination host. Note the "," |
| -e "disk_configuration_enabled=false" | Extra variables to use. This depends on a particular playbook. |

## Required Packages

Ansible comes with a vast number of pre-installed modules. Additional collections used within these roles are:

- ansible.posix        1.3.0  
- community.crypto     1.9.2  
- community.docker     2.0.1  
- community.general    3.5.0  

Collections can be installed using:
```sh
ansible-galaxy collection install <package name>
```

## Project structure

## OS configuration playbooks

### #TODO

## VPN configuration playbooks

### #TODO

