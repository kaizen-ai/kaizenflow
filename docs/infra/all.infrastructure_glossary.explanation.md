# Infrastructure Glossary

<!-- toc -->

- [General technologies](#general-technologies)
  * [Kubernetes](#kubernetes)
  * [Terraform](#terraform)
  * [Ansible](#ansible)
  * [Airflow](#airflow)
  * [Zabbix](#zabbix)
  * [Prometheus](#prometheus)
  * [Helm](#helm)
  * [eksctl](#eksctl)
- [Amazon Web Services (AWS)](#amazon-web-services-aws)
  * [Virtual private cloud (VPC)](#virtual-private-cloud-vpc)
  * [Elastic Kubernetes Service (EKS)](#elastic-kubernetes-service-eks)
  * [Elastic File System (EFS)](#elastic-file-system-efs)
  * [Identity and Access Management (IAM)](#identity-and-access-management-iam)
  * [CloudWatch](#cloudwatch)

<!-- tocstop -->

- This file contains a short description of the technologies we use to manage
  infrastructure
- We refer to further docs for an in-depth analysis

## General technologies

### Kubernetes

- Platform to automate deploying, scaling, and operating containers across a
  cluster of machines
- Supports several container runtimes (e.g., Docker, `containerd`)
- Runs on physical machines, VMs, cloud providers in a consistent way
- Places containers based on resource requirements and constraints to optimize
  resource utilization
- Self-healing
  - Replaces a container if it fails or doesn't respond to health checks
- Horizontal scaling: up and down based on commands, UI, or on resource usage
- Automated rollouts and rollbacks

### Terraform

- Tool for building, changing, and versioning infrastructure
- IaC: users define and provide infrastructure in a configuration file
- Platform agnostic: supports many service providers
- Build a graph of all resources and executes operations in parallel
- Support modules to create reusable components
- Manage state (through a file) mapping real world resources to the
  configuration
- Show a preview of what happens when you modify infrastructure before applying
  the changes

### Ansible

- Automation tool for configuration management, application deployment, and
  service provisioning
- Agentless: doesn't require agent software installed on the nodes to manage,
  but uses SSH
- Playbook describe automation jobs in YAML
- Handles dependencies and roll-back to automate server lifecycle
- Support multi-node deployments
- Work against multiple systems using a list (aka "inventory")

### Airflow

- Tool to schedule and monitor workflows automatically
- Workflows are set up as DAGs to reflect dependencies
- Dynamic pipeline generation in Python
- Scheduler allows a scalable architecture, handling failures and retries
  according to user parameters
- Rich web UI to visualize pipelines, monitor progress, troubleshoot issues
- Many plugins to manage workflows spanning many systems

### Zabbix

- Open-source monitoring solution for network and applications
- Monitor applications metrics, processes, and performance indicators
- Real-time monitoring
- Send alerts through emails, SMS, scripts
- Web-based interface
- Performance based visualization
- Agent and agent-less monitoring

### Prometheus

- Monitoring and alerting toolkit

### Helm

### eksctl

## Amazon Web Services (AWS)

### Virtual private cloud (VPC)

- Provide logically isolated sections of AWS cloud
- You can launch AWS resources
- Resembles a traditional network you operate in your network
  - Select IP address range
  - Create subnets
  - Configure route tables and network gateways
  - Create a public-facing subnet to let access to your web servers
  - Create a private-facing subnet (with no Internet access) for your backend
    systems

### Elastic Kubernetes Service (EKS)

- Run K8 on AWS without installing and maintain K8 control plane or nodes
- Integrates with EC2, IAM, VPC
- Automatic scaling, load balancing
- Provide managed node groups to automate provisioning and lifecycle management
  of nodes (EC2)

### Elastic File System (EFS)

- Scalable, cloud-native file storage service
- Fully managed service
- Elasticity
  - Automatically scale storage capacity and performance
  - Pay only for the storage you use
- Shared file storage
  - Multiple EC2 instances can access an EFS file system
- High durability and availability
  - Store data across multiple availability zones in AWS region
- Different performance modes (general purpose or max I/O)

### Identity and Access Management (IAM)

- Create users and groups permissions to access and deny access to AWS resources
- Permanent or temporary credentials (e.g., expire after a certain duration)
- Multi-factor authentication (besides username and password)
- Can test and validate effects of IAM policies changes before applying them

### CloudWatch

- See
  https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/WhatIsCloudWatch.html

- CloudWatch monitors your AWS resources and the applications you run on AWS in
  real-time
- TODO(gp): Finish
