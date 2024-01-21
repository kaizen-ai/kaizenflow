
<!-- toc -->

- [Set-up AWS Client VPN](#set-up-aws-client-vpn)
  * [Admin Guide](#admin-guide)
  * [User Guide](#user-guide)

<!-- tocstop -->

# Set-up AWS Client VPN

In this guide we are going to set-up AWS client VPN endpoint to allow users to
access private resources.

- The VPN created using this guide is less secure than our OpenVPN self-hosted
  solution (there is no password and 2nd factor authentication)
- It uses mutual authentication of client and server using certificates. Each
  VPN endpoint can hold up to 10 simultaneous connections
- The current set-up assumes that up to 10 users can share client certificate to
  access the VPN endpoint

## Admin Guide

1. Use the following guide to create client/server certificates using `easyrsa`
   and store them inside AWS Certificate Manager

2. Create a client VPN endpoint. TODO(Shayan): update once the template is done

3. Once the endpoint has been created, you can download the client configuration
   file from the endpoint's page in the AWS UI

4. Open the downloaded client configuration file in a text editor

5. Create a section `<cert> </cert>` under `</ca>` and paste inside the content
   of client's `.crt` file created in step 1 (the name of the file depends on
   what you chose in step 1)

6. Create a section `<key> </key>` under `</cert>` and paste the content of
   client's `.key` file created in step 1 (the name of the file depends on what
   you chose in step 1) inside.

7. Distribute the `.ovpn` file using an encrypted zip file
   `zip -e -r downloaded-client-config.ovpn vpn.zip` through company email and
   share the chosen password through a different channel (e.g. via Telegram)

## User Guide

1. Download the AWS VPN client for your computer's OS from the following link
   https://aws.amazon.com/vpn/client-vpn-download/
2. Decrypt the `zip` file received by admin, the zip should contain a file with
   the `.ovpn` extension
3. Connect to the VPN using the following tutorial
   https://docs.aws.amazon.com/vpn/latest/clientvpn-user/connect.html
