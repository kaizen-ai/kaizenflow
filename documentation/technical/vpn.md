<!--ts-->
   * Wireguard client
       * [Install Wireguard](#wireguard-install)
       * [Install Wireguard](#wireguard-install)
       * [Configure Wireguard](#wireguard-configure)
       * [Up/Down VPN](#run-vpn)
   * Wireguard Administation
       * [GUI panel](#admin-panel)
       * CLI commands
   * [IP address](#ip-addresses)
<!--te-->

### Wireguard install

##### Install WireGuard (macOS/Windows)

MacOS / Windows - [Download](https://www.wireguard.com/install/) app.

##### Install WireGuard (Linux/Ubuntu)
``` bash
sudo add-apt-repository ppa:wireguard/wireguard && \
sudo apt update && \
sudo apt install wireguard -y && \
sudo apt install resolvconf -y && \
sudo modprobe wireguard 
```

##### Install WireGuard (Linux/Debian)
``` bash
sudo echo "deb http://deb.debian.org/debian/ unstable main" > /etc/apt/sources.list.d/unstable.list && \
sudo printf 'Package: *\nPin: release a=unstable\nPin-Priority: 90\n' > /etc/apt/preferences.d/limit-unstable && \
sudo apt update && \
sudo apt install wireguard -y && \
sudo apt install resolvconf -y && \
sudo modprobe wireguard 
```

### Wireguard configure

1) Download config (Repo: Commodyti_research, path - infra2/vpn_configs/{username}.conf)

2) macOS/Windows - Set your config, and this is the end. Linux - move your config file to `/etc/wireguard/`

Example: 
`sudo mv gad26032.conf /etc/wireguard/`

### Up / Down VPN:

##### Run VPN Wireguard

MacOS / Windows - Click to icon end set "Connect".

Linux - `wg-quick up {CONFIG NAME} (Without extension .conf)`

Example: 
`sudo wg-quick up gad26032`

##### Down VPN

macOS / Windows - Click to icon end set "Disconnect".

Linux - `wg-quick down {CONFIG NAME} (Without extension .conf)`

Example: `wg-quick down gad26032`


### Wireguard Administration

##### Admin panel

For start work wich admin panel:
 
1) Execute port forwarding:

`ssh -f -nNT -L 3000:localhost:3000 ubuntu@3.12.194.203`

2) Open [localhost:3000](http://localhost:3000/) and enter login/password:
```
Login - particle
Password - 13211321
```

### IP addresses 

```
research-server - 18.190.25.141 (172.31.16.23)
jenkins-server - 52.15.239.182 (172.31.12.239)
mongo-research - 3.14.131.12 (172.31.0.76)
elasticsearch-research - 18.188.198.20 (172.31.6.229)
WireGuard-server - 3.12.194.203 (172.31.13.54)
```