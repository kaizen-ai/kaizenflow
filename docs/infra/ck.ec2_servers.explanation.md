

<!-- toc -->

- [EC2 servers overview](#ec2-servers-overview)
  * [EU-based servers](#eu-based-servers)
    + [dev1 & dev2](#dev1--dev2)
    + [dev3](#dev3)
    + [vpn1](#vpn1)
    + [utility-server](#utility-server)
    + [Airflow1-terraform](#airflow1-terraform)
    + [Zabbix-terraform](#zabbix-terraform)
  * [US-based servers](#us-based-servers)
    + [vpn2](#vpn2)

<!-- tocstop -->

# EC2 servers overview

This list contains all of the EC2 servers utilized for various
business/development processes. Our servers are currently spanning the following
AWS regions:

- Europe (Stockholm) eu-north-1
- US East (N. Virginia) us-east-1

The sections describing individual servers are based on the `Name` attribute of
the EC2 in AWS.

## EU-based servers

### dev1 & dev2

- Development servers available to all team members via SSH
- Available 24/7
- We try to keep team members divided (more or less) equally between the servers

### dev3

- Running only on-demand
- Available to users in case they need to run HW-intensive tasks
- Ask IT team to enable the server for you in case you need it.

### vpn1

- Server hosting a custom VPN solution using OpenVPN + Google auth
  - Acts as a redundancy to the EU VPN
  - Offers lower latency to EU-based team members in comparison to US-based VPN
    server

### utility-server

- Server used to host useful utilities and web apps used for
  development/debugging. Current use cases:
  - pgAdmin - UI interface for Postgres database
  - nginx proxy for `s3://cryptokaizen-html` bucket
    - used for serving Jupyter notebooks, PnL plots, test coverage results, etc

### Airflow1-terraform

- Server hosting Apache Airflow deployment for test and pre-prod stages

### Zabbix-terraform

- Server hosting application monitoring tool Zabbix
  - Zabbix sends informative and warning emails to `infra@crypto-kaizen.com`
    (information about server restart, low disk space, CPU load too high over an
    extended period of time, etc.)

## US-based servers

### vpn2

- Server hosting a custom VPN solution using OpenVPN + Google auth
  - Acts as a redundancy to the EU VPN
  - Offers lower latency to US-based team members in comparison to EU-based VPN
    server
