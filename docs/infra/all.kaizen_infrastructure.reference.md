# Kaizen Infrastructure

This project integrates multiple AWS services and tools to create a robust,
scalable, and automated infrastructure, emphasizing on Terraform for
provisioning and Ansible for configuration management.

## Table of Contents

1. [Introduction](#introduction)
2. [Overview](#overview)
3. [Project Components](#project-components)
   - [AWS Virtual Private Cloud (VPC)](#aws-virtual-private-cloud-vpc)
   - [Elastic Compute Cloud (EC2)](#elastic-compute-cloud-ec2)
   - [Elastic File System (EFS)](#elastic-file-system-efs)
   - [Relational Database Service (RDS)](#relational-database-service-rds)
   - [AWS Simple Storage Service (S3)](#aws-simple-storage-service-s3)
   - [AWS Secrets Manager](#aws-secrets-manager)
   - [Identity and Access Management (IAM)](#identity-and-access-management-iam)
   - [AWS Backup](#aws-backup)
   - [Terraform State Management](#terraform-state-management)
   - [Apache Airflow](#apache-airflow)
   - [OpenVPN](#openvpn)
   - [Server User Management](#server-user-management)
   - [Zabbix](#zabbix)
4. [Setup Instructions](#setup-instructions)
   - [Prerequisites](#prerequisites)
   - [Provider Configuration](#provider-configuration)
   - [Deployment with Terraform](#deployment-with-terraform)
   - [Configuration with Ansible](#configuration-with-ansible)
   - [Main Execution Script](#main-execution-script)
5. [Project Structure](#project-structure)
   - [Terraform Structure](#terraform-structure)
   - [Ansible Structure](#ansible-structure)
6. [Tips & Considerations](#tips-considerations)

## Introduction <a name="introduction"></a>

The architecture spans across various AWS services including VPC, EC2, EFS, RDS,
IAM, AWS Backup, S3, and DynamoDB. It also involves the deployment of OpenVPN
for secure connectivity, Airflow for task orchestration, and Zabbix for
monitoring. The architecture is designed to be robust, automated, and
maintainable.

## Overview <a name="overview"></a>

The infrastructure provisioned through this project encompasses essential AWS
resources, including VPC resources, VPN Server setup, VPN Client setup, Server
user setup, Elastic File System (EFS) setup, RDS (Relational Database Service)
setup, and IAM roles and policies crucial for managing access and permissions
within the AWS environment. Notably, an S3 bucket and DynamoDB table are set up
for Terraform state management, ensuring a reliable and concurrent
infrastructure deployment. The architecture is designed to deploy EC2 instances
tailored for Airflow, Zabbix monitoring solutions, OpenVPN for secure VPN
connectivity, and an RDS instance for PostgreSQL database management.

While Airflow facilitates modern task orchestration, Zabbix monitors the health
and performance of the Airflow EC2 instance and the Docker containers running
Airflow tasks. OpenVPN provides both server and client setups to ensure secure
access to resources. The VPN server and client setups also incorporate
Multi-Factor Authentication (MFA) using Google Authenticator, ensuring enhanced
security. The RDS setup involves provisioning database instances, setting up
database subnet groups, and ensuring high availability and robust security
configurations. The IAM setup is instrumental in defining and applying
permissions for different AWS services, ensuring secure and principle-based
access control.

The configurations ensure the application and its dependencies are correctly
installed and configured on the provisioned resources. The configuration deploys
Airflow on the EC2 instance, ensuring it's properly configured with required
dependencies such as Docker, Docker Compose, and integration with AWS EFS for
DAG storage. Zabbix, on the other hand, provides real-time monitoring metrics,
which can be invaluable for diagnosing issues and ensuring optimal performance.
The VPN server and client setups ensure secure remote access to the
infrastructure and its resources.

The architecture also provides tasks for server user management, allowing for
the addition and removal of users while ensuring appropriate group associations
and SSH key setups. The project utilizes AWS Secrets Manager for secure
management of secrets, which includes credentials for PostgreSQL, Airflow
encryption keys, and SMTP settings. This makes scaling, replicating, and
maintaining the Airflow environment more manageable and reliable. Additionally,
AWS Backup is integrated to ensure the persistent and reliable backup of
resources, particularly EC2 instances.

## Project Components <a name="project-components"></a>

### AWS Virtual Private Cloud (VPC) <a name="aws-virtual-private-cloud-vpc"></a>

AWS VPC serves as the backbone of the network architecture, ensuring resources
are logically isolated within the AWS cloud. The Terraform configuration
includes VPC creation, subnet provisioning, Internet Gateway, NAT Gateway, route
tables, and security groups.

### Elastic Compute Cloud (EC2) <a name="elastic-compute-cloud-ec2"></a>

The EC2 setup provides scalable computing capacity for hosting applications like
Airflow, Zabbix, and VPN Server. The Terraform configuration covers instance
provisioning, key pair setup, and security group assignments.

### Elastic File System (EFS) <a name="elastic-file-system-efs"></a>

AWS EFS is used for maintaining the persistent storage required by Airflow for
its Directed Acyclic Graphs (DAGs). This makes sure that regardless of the
lifecycle of the EC2 instances where Airflow runs, the DAG definitions and their
histories remain intact. The EFS setup in the Terraform structure ensures the
creation of the EFS file system and the required mount targets, so EC2 instances
can access it.

### Relational Database Service (RDS) <a name="relational-database-service-rds"></a>

The RDS setup in the project is designed to provide a managed PostgreSQL
database service. The Terraform configuration setup involves provisioning of
database instances, setting up database subnet groups, and ensuring high
availability and robust security configurations. The Ansible structure for RDS
ensures the correct configuration of the PostgreSQL database with the necessary
user permissions and roles.

### AWS Simple Storage Service (S3) <a name="aws-simple-storage-service-s3"></a>

AWS S3 is utilized for scalable object storage, serving as a backbone for
storing and retrieving any amount of data, at any time, from anywhere on the
web. S3 is crucial for managing various types of data including backups, logs,
and static files. The Terraform configuration automates the creation of S3
buckets with specific access controls, lifecycle rules for managing object
storage, and versioning for data durability. It also sets up server-side
encryption for security, replication for cross-region data availability, and
public access blocks to prevent unauthorized access.

### AWS Secrets Manager <a name="aws-secrets-manager"></a>

AWS Secrets Manager is utilized for secure management of secrets, ensuring
scalable, replicable, and maintainable environments.

### Identity and Access Management (IAM) <a name="identity-and-access-management-iam"></a>

AWS IAM allows fine-grained control over AWS resources, ensuring only permitted
actions can be taken on the resources by authorized entities. The Terraform
module for IAM creates roles, policies, and instance profiles. This module is
essential for securing and managing access across all the AWS resources
provisioned by the Terraform scripts.

### AWS Backup <a name="aws-backup"></a>

AWS Backup is integrated to offer centralized backup across AWS services. The
Terraform module handles the creation of backup vaults, backup plans, and backup
selections. The module enables scheduled backups for resources, allowing
lifecycle configurations for storage transitions and deletions. By setting up
AWS Backup, the infrastructure ensures data durability and quick recovery in
case of disruptions.

### Terraform State Management <a name="terraform-state-management"></a>

The S3 and DynamoDB in `terraform/bootstrap` directory, serve as the backbone
for managing the state of the Terraform deployments. S3 is used to store the
actual state file, while DynamoDB is used to lock the state file during
modifications, ensuring concurrent deployments don't cause conflicts. The
Terraform configuration involves bucket creation and configuration for state
files, and setting up the DynamoDB table for state locking.

### Apache Airflow <a name="apache-airflow"></a>

Apache Airflow manages and executes batch jobs and workflows, crucial for task
orchestration. The Ansible configuration covers deployment setup, environment
configuration, and EFS integration for DAGs.

### OpenVPN <a name="openvpn"></a>

OpenVPN facilitates secure and encrypted connections into the network
infrastructure. The Ansible configuration includes both server and client setup,
integrating Multi-Factor Authentication (MFA) for enhanced security.

### Server User Management <a name="server-user-management"></a>

The Server User Management tasks in Ansible configuration provides adding and
removing server users, ensuring appropriate group associations and SSH key
setups.

### Zabbix <a name="zabbix"></a>

Zabbix monitors the health and performance of infrastructure components,
particularly EC2 instances, Airflow tasks, and docker containers. The Ansible
configuration encompasses server and agent setup for effective monitoring.

## Setup Instructions <a name="setup-instructions"></a>

### Prerequisites <a name="prerequisites"></a>

- **Terraform** v1.5 or later. You can download it from
  [here](https://developer.hashicorp.com/terraform/downloads).
- **Ansible** v2.15 or later. You can download it from
  [here](https://developer.hashicorp.com/terraform/downloads).
- **AWS CLI** v2 or later. You can download it from
  [here](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

### Provider Configuration <a name="provider-configuration"></a>

- This project leverages the AWS provider version `~> 4.0`. It's pre-configured
  for the `eu-north-1` region and uses the `ck` AWS profile. Ensure the profile
  has appropriate permissions.

- Prior to execution, ensure your AWS credentials are properly set up.
  Instructions on setting up AWS credentials can be found
  [here](https://registry.terraform.io/providers/hashicorp/aws/latest/docs).

## Deployment with Terraform <a name="deployment-with-terraform"></a>

1. Clone this repository to your local machine.
   ```bash
   git clone https://github.com/cryptokaizen/cmamp.git
   ```
2. **Bootstrap:** Navigate to the terraform/bootstrap director
   ```bash
   cd terraform/bootstrap
   ```
3. Initialize Terraform and create the S3 bucket and DynamoDB table resources
   for state management.
   ```bash
   terraform init
   terraform apply
   ```
4. **Infrastructure Provisioning:** Move back to terraform directory.
   ```bash
   cd ..
   ```
5. Initialize Terraform and deploy the VPC resources and EC2 instances along
   with associated key pairs and other resources. Terraform will prompt for
   confirmation before applying the changes. Type `yes` to proceed.
   ```bash
   terraform init
   terraform apply
   ```
6. Post deployment, you will have the necessary EC2 key pairs retrieved in the
   `terraform/modules/ec2` directory. These keys are critical for Ansible and
   should be securely managed.

7. **Clean Up (Optional):** If you wish to remove the resources created by
   Terraform, you can use:
   ```bash
   terraform destroy
   ```

## Configuration with Ansible <a name="configuration-with-ansible"></a>

1. **Navigate to Ansible Directory:** Once the AWS resources are provisioned,
   navigate to the `ansible` directory.

   ```bash
   cd ansible
   ```

2. **Capture VPN Public IP from Terraform:** After applying the main Terraform
   configurations, extract the public IP of the VPN server from the Terraform
   outputs and update the `ansible/inventory/hosts.ini` file.

   ```bash
   VPN_PUBLIC_IP=$(terraform output -json vpn_server_public_ip | jq -r '.["VPN-terraform"]')
   sed -i "s/^vpn_host ansible_host=[0-9.]\+/vpn_host ansible_host=${VPN_PUBLIC_IP}/" "${DEPLOY_DIR}/ansible/inventory/hosts.ini"
   ```

3. **Execute the VPN Server Playbook:** This step configures the VPN server.

   ```bash
   ansible-playbook -i inventory/hosts.ini playbooks/vpn_server.yml -e "vpn_region=eu"
   ```

4. **Execute the VPN Client Playbook:** This step sets up the VPN client. Ensure
   you replace the placeholders with appropriate values.

   ```bash
   ansible-playbook -i inventory/hosts.ini playbooks/vpn_client.yml -e "openvpn_client_username=${OPENVPN_USERNAME}" -e "vpn_client_action=add" -e "vpn_region=eu" -e "openvpn_server_remote_host=${VPN_PUBLIC_IP}"
   ```

5. **Establish VPN Connection:** Use the generated OTP and establish a VPN
   connection.

6. **Execute the Airflow Playbook:** Configure the Airflow EC2 instance and
   install the necessary dependencies. Ensure you replace the placeholders
   `${GITHUB_USER}`, `${GITHUB_PAT}`, `${DEPLOYMENT_ENV}`, and `${EFS_DNS_NAME}`
   with appropriate values.

   ```bash
   ansible-playbook -i inventory/hosts.ini playbooks/airflow.yml -e "github_user=${GITHUB_USER}" -e "github_pat=${GITHUB_PAT}" -e "deployment_env=${DEPLOYMENT_ENV}" -e "efs_dns_name=${EFS_DNS_NAME}"
   ```

7. **Execute the Zabbix Server Playbook:** Configure the Zabbix server on its
   respective EC2 instance.

   ```bash
   ansible-playbook -i inventory/hosts.ini playbooks/zabbix_server.yml
   ```

8. **Disconnect from the VPN Server (Optional):** After all the configurations
   are done, you can disconnect from the VPN server.
   ```bash
   openvpn3 session-manage --config $OVPN_FILE --disconnect
   ```

## Main Execution Script <a name="main-execution-script"></a>

The project also provides a unified script `main.sh` that can be used for both
deploying and destroying the infrastructure. This script is designed to check
for prerequisites, deploy the Terraform configurations, establish a VPN
connection, and run the Ansible playbooks seamlessly. Ensure you run it from the
project's root directory.

```bash
sudo bash main.sh
```

## Project Structure <a name="project-structure"></a>

The project is structured to separate concerns and maintain a clear hierarchy.
The major divisions include Terraform, responsible for infrastructure
provisioning, and Ansible, which takes care of configuration management.

### Terraform Structure <a name="terraform-structure"></a>

The Terraform part of the project deals with the setup of major AWS components
such as EC2, VPC, EFS, RDS, and others. Each of these components has its own
module, detailing specific configurations and resource provisioning.

- **terraform/:** The root directory housing all Terraform configurations.
  - **main.tf:** Contains module references for EC2, VPC, EFS, RDS, and
    associated configurations.
  - **provider.tf:** Specifies required providers and backend configurations.
  - **terraform.tfvars:** Houses variable definitions for the Terraform setup.
  - **bootstrap/:** Contains Terraform configurations for setting up the S3
    bucket and DynamoDB table for state management.
  - **modules/ec2/:** Contains resources related to EC2 provisioning, which
    include instances, key pairs, etc.
  - **modules/efs/:** Contains resources and configurations specific to AWS EFS.
    This module manages:
    - **EFS File System Creation:** Defines the EFS file system with associated
      parameters like performance mode, encryption settings, and throughput
      mode.
    - **EFS Mount Targets:** Provisions mount targets within specified subnets,
      allowing EC2 instances within those subnets to mount the EFS file system.
  - **modules/rds/:** Contains resources and configurations specific to AWS RDS.
    This module manages:
    - **RDS Subnet Group Creation:** Defines the RDS subnet group with an
      associated description, name, and subnet IDs.
    - **RDS Database Instance:** Provisions the RDS database instance with
      configurations like instance class, engine, username/password, backup
      policies, availability settings, storage, and security configurations.
  - **modules/vpc/:** Contains resources and configurations specific to AWS
    VPCs. This module manages:
    - **VPC Creation:** Defines the main VPC with associated parameters.
    - **Subnets:** Provisions subnets within the VPC, allowing for multi-zone
      redundancy.
    - **Internet Gateway:** Creates an Internet Gateway and associates it with
      the VPC.
    - **NAT Gateway:** Sets up NAT gateways with allocated Elastic IPs.
    - **Route Tables & Associations:** Manages route tables, their associations
      with subnets, and their routing rules.
    - **Security Groups & Rules:** Defines security groups and their ingress and
      egress rules.
    - **Local Mappings:** The module also uses local values to create maps for
      security group names to IDs, subnet names to IDs, and route table names to
      IDs for ease of reference.
  - **modules/iam/:** This module contains the Terraform resources related to
    IAM. This module uses templating for dynamic policy creation, accommodating
    different resource specifications. It manages:
    - **IAM Roles:** Creation of IAM roles with specific permissions and
      policies. Each role is defined with an appropriate trust relationship
      policy to specify who or what can assume the role.
    - **IAM Policies:** Definitions of IAM policies in JSON format, specifying
      the permissions that can be attached to various AWS resources or users.
    - **IAM Role-Policy Attachments:** Associations between IAM roles and
      policies, ensuring that the roles have the required permissions to
      function effectively.
    - **IAM Instance Profiles:** Creation of instance profiles that can be used
      to pass IAM roles to EC2 instances, thereby granting them permissions to
      access other AWS resources.
  - **modules/aws_backup/:** This module handles the AWS Backup service,
    encompassing:
    - **Backup Vault Creation:** Provisions the backup vault with a specific
      name and provides an option for forced destruction.
    - **Backup Plan:** Configures the backup plan which includes backup
      schedules, lifecycle policies, and associates it with a backup vault.
    - **Backup Selection:** Specifies which AWS resources should be backed up,
      based on either direct resource listing or tags. An IAM role ARN is
      provided for required permissions during backup operations.
  - **modules/s3/:** Contains resources and configurations specific to AWS S3.
    This module manages various aspects of S3 bucket configuration including:
    - **S3 Bucket Creation:** Defines the S3 buckets with parameters like name,
      ACL, and force destruction options.
    - **Bucket Versioning:** Enables versioning to maintain a history of object
      changes.
    - **Lifecycle Rules:** Implements rules for automatic management of objects,
      such as archiving to lower-cost storage options or deleting outdated
      items.
    - **Server-Side Encryption:** Sets up encryption for data security.
    - **Replication Configuration:** Manages data replication across different
      S3 buckets for enhanced availability and backup.
    - **Public Access Block:** Implements settings to block public access to the
      buckets.
    - **Bucket Policy:** Uses templated JSON policies for applying custom bucket
      policies.

### Ansible Structure <a name="ansible-structure"></a>

Ansible is used for configuration management. This section breaks down the
Ansible roles, each responsible for setting up and configuring different
components like Airflow, Zabbix, OpenVPN, and RDS among others.

- **ansible/:** The root directory housing all Ansible configurations.
  - **ansible.cfg:** This is the global configuration file for Ansible. It
    defines essential configurations like the location of the inventory, private
    key file, and more.
  - **inventory/hosts.ini:** Defines target hosts. Ensure you update
    `ansible_host` to reflect the actual IP of your provisioned EC2 instance.
  - **roles/:** Contains roles which are sets of Ansible tasks grouped together.
    - **common/:** Contains general setup tasks that any instance may require:
      package updates, Docker setup, repository cloning, and firewall settings.
      Variables `common/vars/main.yml` include settings like Docker URLs and
      GitHub repository details. Notably, the GitHub username and PAT (Personal
      Access Token) are expected to be passed in as environment variables,
      suggesting a security-conscious approach.
    - **airflow/:** Handles tasks specific to the Airflow deployment. These
      tasks include setting up `.env` files, configuring Docker Compose,
      starting up Airflow services, and handling data persistence with EFS.
      Variables `airflow/vars/main.yml` are sourced from AWS Secrets Manager, a
      good practice for secret management. These variables include PostgreSQL
      credentials, Airflow encryption keys, and SMTP settings.
    - **zabbix_agent/:** Contains tasks for setting up the Zabbix agent,
      essential for collecting data about the system and relaying it to the
      Zabbix server.
    - **zabbix_server/:** Contains tasks for setting up and configuring the
      Zabbix server, which collects and displays the monitored data.
    - **vpn_server/:** Contains tasks specifically for setting up and
      configuring an OpenVPN server, along with Multi-Factor Authentication
      (MFA) using Google Authenticator. This ensures a secure VPN connection for
      users. The setup includes the creation of the Public Key Infrastructure
      (PKI) directory for OpenVPN, enabling of IPv4 traffic forwarding, and
      deployment of the OpenVPN server configuration. The Google Authenticator
      setup entails creating a service group and user, configuring the PAM
      module, and handling MFA tokens. To facilitate region-specific
      configurations, tasks dynamically load VPN server variables based on the
      specified region (e.g., 'eu', 'us').
    - **vpn_client/:** Contains tasks tailored for setting up and configuring an
      OpenVPN client with Multi-Factor Authentication (MFA) using Google
      Authenticator. The tasks facilitate the addition of VPN users by creating
      operating system users for MFA, setting up Google Authenticator for each
      user, and generating necessary VPN client configurations. It also ensures
      a seamless flow for region-based configurations by loading region-specific
      variables dynamically. The role also provides tasks for the removal of VPN
      users, revoking their access, and cleaning up their configurations.
    - **server_user/:** Contains tasks for managing (addition and removal) user
      accounts on the server. It ensures the user's group and home directory are
      appropriately set up, assigns users to the Docker group if Docker is
      running, appends user's public key to authorized_keys for SSH access, and
      records server IPs. The tasks are conditional based on the
      `server_user_action` variable value, which determines if the user should
      be added or removed.
    - **rds_setup/:** Handles tasks specific to RDS setup and configuration.
      These tasks include installing necessary packages, creating PostgreSQL
      databases, and setting up users with different privileges (read/write and
      read-only). Variables in `rds_setup/vars/main.yml` are fetched from AWS
      Secrets Manager, promoting secure secret management. These variables
      include database usernames, passwords, and other sensitive information.
      The role provisions PostgreSQL databases based on the deployment
      environment and predefined prefix and creates both Read/Write and
      Read-Only users for the provisioned database.
  - **playbooks/:** Contains playbooks to execute the Ansible roles.
    - **airflow.yml:** Playbook to set up and deploy Airflow and the Zabbix
      agent on the Airflow EC2 instance.
    - **zabbix_server.yml:** Playbook to set up the Zabbix server.
    - **vpn_server.yml:** Playbook designed to execute the tasks associated with
      the `vpn_server` role and configures the Zabbix agent on the provisioned
      VPN Server.
    - **vpn_client.yml:** Playbook designed to execute the tasks associated with
      the `vpn_client` role. It sets up an OpenVPN client, ensures that
      region-specific configurations are loaded, manages user addition and user
      removal based on the value of `vpn_client_action` (either "add" for user
      addition or "remove" for user removal).
    - **server_user.yml:** A playbook designed to execute the tasks associated
      with the `server_user` role. It manages server user accounts by either
      adding or removing them based on the `server_user_action` variable (either
      "add" for user addition or "remove" for user removal).
    - **rds_setup.yml:** Playbook to set up RDS instances, ensuring databases
      are created and configured with the necessary user permissions and roles.

## Tips & Considerations <a name="tips-considerations"></a>

- **Deployment Specification:** Before applying the deployment, ensure the
  `terraform/terraform.tfvars` file is updated with appropriate values.
- **Environment File Configuration:** Ensure the `.env` file is accurately
  filled with the necessary environment configurations for Airflow. This file
  (`ansible/roles/airflow/templates/.env.j2`) must be templated and the
  variables substituted with actual values before Airflow is started.
- **Secure Key Management:** Ensure that the EC2 key pairs retrieved
  post-deployment are securely managed.
- **Infrastructure Dependencies:** Always execute the Terraform scripts in the
  order of bootstrap first followed by the main terraform directory to ensure
  correct dependency resolution.
- **Access:** The EC2 instance provisioned will only be accessible with the
  appropriate VPN client due to the VPC configurations.
- **Plan the Deployment:** Always perform a `terraform plan` before a
  `terraform apply` to preview changes.
- **Security Notes:** Even though the project has `host_key_checking = False`
  for convenience, in production, it's best to ensure host keys are verified to
  avoid MITM attacks.
- **Firewall:** Disabling the firewall can expose the instance to
  vulnerabilities. Make sure to have other security measures in place or
  reconsider this configuration.
- **Telegram Chat ID:** If a basic group reaches a certain member count or if
  some specific features are used (like the deletion of group messages by
  admins), it's automatically converted to a supergroup by Telegram, and this
  could cause the chat ID to change. To get the chat ID for a Telegram group (or
  channel), you can use the `getUpdates` method provided by the Telegram API.
  ```bash
  curl -s "https://api.telegram.org/botYOUR_BOT_TOKEN/getUpdates"
  ```
- **Troubleshooting and Testing the Telegram Bot:** We can manually send a
  message using the Telegram Bot API to check if it's working:
  ```bash
  curl -X POST "https://api.telegram.org/botTELEGRAM_BOT_TOKEN/sendMessage" -d "chat_id=THE_CHAT_ID&text=Test Message"
  ```
