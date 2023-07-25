# HTMLcov server

<!-- toc -->

- [HTMLcov sever](#HTMLcov-sever)
  * [Proxy address to the s3 bucket](#Proxy-address-to-the-s3-bucket)
- [Usage](#usage)
- [Admin Configuration Info](#admin-configuration-info)
  * [S3 bucket](#s3-bucket)
  * [NGINX server](#nginx-server)

<!-- tocstop -->

## Proxy address to the s3 bucket

- [http://172.30.2.44](http://172.30.2.44) (available only behind VPN)

## Usage

1. Upload the coverage into bucket: **cryptokaizen-html** (located on CK AWS
   account), note the syntax from CLI:
   - `aws s3 cp ./htmlcov s3://cryptokaizen-html/juraj_test --recursive --profile ck`
2. Access the coverage via browser:
   - [http://172.30.2.44/juraj_test](http://172.30.2.44/juraj_test)
3. Nesting example
   - Upload at /htmlcov `s3://cryptokaizen-html/juraj_test2/CmTask8789`
   - Access at
     [http://172.30.2.44/juraj_test/CmTask8789](http://172.30.2.44/juraj_test/CmTask8789)

## Admin Configuration Info

### S3 bucket

- Need to set bucket policy to only allow `getObject` operations from one IP:
  (which is a NAT GATEWAY of our VPC)
  - IMPORTANT STEP OTHERWISE THE SITE IS VISIBLE PUBLICLY
  
  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "AllowReadFromNATGateway",
        "Effect": "Allow",
        "Principal": "*",
        "Action": "s3:GetObject",
        "Resource": "arn:aws:s3:::cryptokaizen-html/*",
        "Condition": {
          "IpAddress": {
            "aws:SourceIp": "16.170.193.70/32"
          }
        }
      }
    ]
  }
  ```
- Public access can be blocked:
  - ![NGINX server](Tools_htmlcov_server_figs/image1.png)

### NGINX server

- [Juraj Smeriga](mailto:jsmeriga@crypto-kaizen.com) and
  [GP](mailto:gp@crypto-kaizen.com) have their public key stored on
  the server in case access via ssh is needed (user: ubuntu)
- Simple t3.nano instance (run ansible hardening playbook for additional
  security)
- Nginx installed with simple config added into `/etc/nginx/sites-enabled/default`
  inside default server configuration:

  ```bash
  location / {
      proxy_pass http://cryptokaizen-html.s3-website.eu-north-1.amazonaws.com/;
  }
  ```

- After any change run `systemctl restart nginx`
