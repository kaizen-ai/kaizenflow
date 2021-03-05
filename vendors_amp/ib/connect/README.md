# Interactive Brokers Gateway Docker

IB Gateway running in Docker with [IB Controller](https://github.com/ib-controller/ib-controller/) and VNC

* TWS Gateway: v974.4g
* IB Controller: v3.2.0

## Getting Started

All commands runs from `amp` root directory.

### Pull image

```bash
> make docker_login
> make ib_connect.docker.pull
```

If image is not at the production stage look [Create image](create-image).

### Create image

```bash
> make ib_connect.docker.build.rc_image
> # Check that image is correct.
> make ib_connect.docker.tag.rc.latest
```

### Set up VNC server with running app on localhost

You will need to add a file with IB credentials to `~/.vnc/ib.credentials` like:
```bash
TWSUSERID=user123
TWSPASSWORD=password456
FIXUSERID=
FIXPASSWORD=
```

#### Start TWS

```bash
> IB_CONNECT_VNC_PASSWORD=12345 IB_CONNECT_APP=TWS make ib_connect.docker.up
```

#### Start Gateway app

```bash
> IB_CONNECT_VNC_PASSWORD=12345 IB_CONNECT_APP=GATEWAY make ib_connect.docker.up
```

You will now have the IB Gateway app running on port 4003 and VNC on 5901.

### Set up VNC server with running app on remote server

In addition to [Set up VNC server with running app on localhost](set-up-vnc-server-with-running-app-on-localhost)
you will still need to add a parameter `IB_CONNECT_TRUSTED_IPS` 
with all public IP-s needed to connect to VNC server (e.g. IP of your local machine):

```bash
> IB_CONNECT_VNC_PASSWORD=12345 IB_CONNECT_TRUSTED_IPS=46.73.103.55 IB_CONNECT_APP=GATEWAY make ib_connect.docker.up
```

#### Start Gateway app

```bash
> IB_CONNECT_VNC_PASSWORD=12345 IB_CONNECT_APP=GATEWAY make ib_connect.docker.up
```

You will now have the IB Gateway app running on port 4003 and VNC on 5901.



### Shutdown VNC server with running app

```bash
> make ib_connect.docker.down
```

## Client connection to running TWS/Gateway app

To connect to VNC where the app is running, one will need a VNC viewer app.
Gateway app example:
![vnc](docs/ib_gateway_vnc.jpg)

How to connect to VNC with different OS is described here. 
Assume that VNC server is running on localhost:5901.

### Linux

```bash
sudo apt install tigervnc-viewer
vncviewer localhost::5901
```

It will ask you for a password, use one that you use on VNS server start up (`IB_CONNECT_VNC_PASSWORD`).
