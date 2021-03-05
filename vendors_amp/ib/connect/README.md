<!--ts-->
   * [Interactive Brokers Gateway Docker](#interactive-brokers-gateway-docker)
      * [Getting Started](#getting-started)
         * [Pull image](#pull-image)
         * [Create image](#create-image)
         * [Set up VNC server with running app on localhost](#set-up-vnc-server-with-running-app-on-localhost)
            * [Start TWS](#start-tws)
            * [Start Gateway app](#start-gateway-app)
         * [Set up VNC server with running app on remote server](#set-up-vnc-server-with-running-app-on-remote-server)
            * [Start Gateway app](#start-gateway-app-1)
         * [Shutdown VNC server with running app](#shutdown-vnc-server-with-running-app)
      * [Client connection to running TWS/Gateway app](#client-connection-to-running-twsgateway-app)
         * [Linux](#linux)



<!--te-->

# Interactive Brokers Gateway Docker

- Docker container with:
  - TWS Gateway: v974.4g
  - IB Controller: v3.2.0
  - [IB Controller](https://github.com/ib-controller/ib-controller/)
  - VNC

## Getting Started

- All commands need to be executed from `amp` root directory

### Pull image

```bash
> make docker_login
> make ib_connect.docker.pull
```

- If the image is not available in ECR you can follow the instructions in [Create
  image](create-image)

### Create image

```bash
> make ib_connect.docker.build.rc_image
> # Check that image is correct.
> make ib_connect.docker.tag.rc.latest
```

### Start app on localhost

- You need to add a file with IB credentials to `~/.vnc/ib.credentials` like:
  ```bash
  TWSUSERID=user123
  TWSPASSWORD=password456
  FIXUSERID=
  FIXPASSWORD=
  ```

- Start TWS
  ```bash
  > IB_CONNECT_VNC_PASSWORD=12345 IB_CONNECT_APP=TWS make ib_connect.docker.local.up
  ```

- Start Gateway app
  ```bash
  > IB_CONNECT_VNC_PASSWORD=12345 IB_CONNECT_APP=GATEWAY make ib_connect.docker.local.up
  ```

- You will now have the IB Gateway app running on port 4003 and VNC on 5901.

- Note that you might need to add a parameter `IB_CONNECT_TRUSTED_IPS` with all
  public IP-s needed to connect to VNC server (e.g. IP of your local machine):
  ```bash
  > MY_IP=$(curl ifconfig.me); IB_CONNECT_VNC_PASSWORD=12345 IB_CONNECT_TRUSTED_IPS=$MY_IP IB_CONNECT_APP=TWS make ib_connect.docker.local.up
  ```

### Shutdown app

```bash
> make ib_connect.docker.local.down
```

# VNC

- To connect through VNC to the app, one needs a VNC viewer app

- Assume that VNC server is running on `localhost:5901`
- As a password, use one that you use on VNC server start up
  `IB_CONNECT_VNC_PASSWORD`

## Linux

- Install a VNC viewer with:
  ```bash
  > sudo apt install tigervnc-viewer
  ```

- Start the VNC viewer with:
  ```bash
  > vncviewer localhost::5901
  ```

## Mac

- You can use the embedded VNC viewer by going to Finder
- In the menu, `Go` -> `Connect to server...` enter
  - Go to `vnc://localhost:5901`
  - Then you should get prompted for the password

- As an alternative you can use 
