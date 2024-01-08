

<!-- toc -->

- [Utility server application set-up](#utility-server-application-set-up)
  * [Prerequisites](#prerequisites)
  * [Guide](#guide)
    + [Install NGINX](#istall-nginx)
    + [run pgAdmin](#run-pgadmin)
    + [Enable proxy to S3](#enable-proxy-to-s3)

<!-- tocstop -->

# Utility server application set-up

This guide provides steps to install and set up applications hosted on the
utility server:

- pgAdmin
- Proxy to serve S3 bucket contents privately
  - Example:
    - There is a server accessible only through a VPN with a private IP
      `172.30.2.44`
    - There is a file stored at `s3://cryptokaizen-html/pnl_plots/my_pnl.png`, not
      accessible publicly.
    - Using the NGINX proxy the file will be accessible using the URL
      `http://172.30.2.44/pnl_plots/my_pnl.png`

## Prerequisites

- Sudo access to an EC2 instance with an Ubuntu 20 OS
  - It is assumed the server is running in a private subnet and a user uses VPN
    to access it
- S3 bucket with public access DISABLED and static website hosting enabled
- Permissions to alter IAM policies of the S3 bucket

## Guide

### Install NGINX

- follow the tutorial
  https://www.digitalocean.com/community/tutorials/how-to-install-nginx-on-ubuntu-20-04

### run pgAdmin

- log into the server:

```bash
> ssh ubuntu@<<your_server_ip>>
```

- Run docker container

```bash
> docker run -p 5050:80 \
-e 'PGADMIN_DEFAULT_EMAIL=<<your_email>>' \
-e 'PGADMIN_DEFAULT_PASSWORD=<<stored in 1password>> \
-e "PGADMIN_LISTEN_ADDRESS=0.0.0.0" \
-d dpage/pgadmin4
```

- Open the default NGINX server entry in a text editor

```bash
> vim /etc/nginx/sites-enabled/default
```

- Add an entry to set reverse proxy

```
location /pgadmin/ {
        proxy_set_header X-Script-Name /pgadmin;
        proxy_set_header Host $host;
        proxy_pass http://localhost:5050/;
        proxy_redirect off;
}
```

- Restart NGINX

```bash
sudo systemctl restart nginx
```

- Confirm pgadmin is accessible via `http://<<your_server_ip>>/pgadmin/login`

### Enable proxy to S3

- Open the default NGINX server entry in a text editor

```bash
> vim /etc/nginx/sites-enabled/default
```

- Add an entry to set reverse proxy

```
location / {
    proxy_pass http://<<your_bucket_name>>.s3-website.<<aws_region_of_your_bucket>>.amazonaws.com/;
    }
```

- Restart NGINX

```bash
sudo systemctl restart nginx
```
