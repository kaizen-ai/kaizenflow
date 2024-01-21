<!--ts-->
   * [Interactive Brokers Gateway Docker](#interactive-brokers-gateway-docker)
      * [Getting Started](#getting-started)
      * [Create image](#create-image)
         * [Start app on localhost](#start-app-on-localhost)
         * [Start app on production](#start-app-on-production)
         * [Additional start parameters](#additional-start-parameters)
         * [Shutdown app](#shutdown-app)
   * [VNC](#vnc)
      * [Linux](#linux)
      * [Mac](#mac)



<!--te-->

# Interactive Brokers Gateway Docker

- This is a Docker container with:
  - TWS Gateway: v974.4g
  - [IB Controller](https://github.com/ib-controller/ib-controller/)
  - VNC

## Getting Started

- All commands need to be executed from the current directory
  ```bash
  > cd <amp>/im/ib/connect`
  ```

## Create image

- To build the `im_tws:local` image

  ```bash
  > invoke docker_build_local_image
  ```

- To release the image:
  ```bash
  > invoke docker_push_local_image_to_dev
  ```

<!---
### Pull image

```bash
> make ib_connect.docker_pull
```

- If the image is not available in ECR you can follow the instructions in
  [Create image](create-image)

### Build image

```bash
> make ib_connect.docker_build_image.rc
> # .. Check that image is correct ...
...
> make ib_connect.docker_tag_latest.rc
```
-->

### Start app on localhost

- Note that only one TWS app can be opened at the same time
  - If the production app is up, no one can run the app even in local mode

- You need to add a file with IB credentials to `~/.vnc/ib.credentials` like:

  ```bash
  TWSUSERID=user123
  TWSPASSWORD=password456
  FIXUSERID=
  FIXPASSWORD=
  ```

- Start TWS

  ```bash
  > IB_CONNECT_VNC_PASSWORD=12345 IB_CONNECT_APP=TWS make ib_connect.docker_up.local
  ```

- Start Gateway app

  ```bash
  > IB_CONNECT_VNC_PASSWORD=12345 IB_CONNECT_APP=GATEWAY make ib_connect.docker_up.local
  ```

- You will now have the IB Gateway app running on port 4003 and VNC on 5901. To
  specify custom ports run in a following way:

  ```bash
  > IB_CONNECT_API_PORT=4015 IB_CONNECT_VNC_PORT=5915 IB_CONNECT_VNC_PASSWORD=12345 make ib_connect.docker_up.local
  ```

### Start app on production

- Start TWS

  ```bash
  > IB_CONNECT_USER=user123 IB_CONNECT_PASSWORD=password456 IB_CONNECT_VNC_PASSWORD=12345 IB_CONNECT_APP=TWS make ib_connect.docker_up.prod
  ```

- Start Gateway app

  ```bash
  > IB_CONNECT_USER=user123 IB_CONNECT_PASSWORD=password456 IB_CONNECT_VNC_PASSWORD=12345 IB_CONNECT_APP=GATEWAY make ib_connect.docker_up.prod
  ```

- You will now have the IB Gateway app running on port 4004 and VNC on 5902.

### Additional start parameters

- Note that you might need to add a parameter `IB_CONNECT_TRUSTED_IPS` with all
  public IP-s needed to connect to VNC server (e.g. IP of your local machine):
  ```bash
  > MY_IP=$(curl ifconfig.me); IB_CONNECT_VNC_PASSWORD=12345 IB_CONNECT_TRUSTED_IPS=$MY_IP IB_CONNECT_APP=TWS make ib_connect.docker_up.local
  ```

### Shutdown app

- Local image

  ```bash
  > make ib_connect.docker_down.local
  ```

- Production image
  ```bash
  > make ib_connect.docker_down.prod
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
- From command line
  - `open vnc://localhost:5901`
