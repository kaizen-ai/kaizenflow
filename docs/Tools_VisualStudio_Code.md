# Tools - VisualStudio Code

<!-- toc -->
- [Connecting via VNC](#connecting-via-vnc)
  * [Installing VNC](#installing-vnc)
- [Installation of VS Code](#installation-of-vs-code)
  * [Windows, Linux, Mac](#windows-linux-max)  
<!-- tocstop -->

# Connecting via VNC

- Make sure you have a VPN connection.

## Installing VNC

- Install VNC using this link: [<span
  class="underline">https://www.realvnc.com/en/connect/download/viewer/windows/</span>](https://www.realvnc.com/en/connect/download/viewer/windows/)
- Sysadmin has sent you:
  - `os_password.txt`
  - your username `$USER`
  - a key `crypto.pub` that looks like:

```
-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAABlwAAAAdzc2gtcn
NhAAAAAwEAAQAAAYEA0IQsLy1lL3bhPT+43sht2/m9tqZm8sEQrXMAVtfm4ji/LXMr7094
…
hakqVTlQ2sr0YTAAAAHnNhZ2dlc2VAZ3BtYWMuZmlvcy1yb3V0ZXIuaG9tZQECAwQ=
-----END OPENSSH PRIVATE KEY-----
```

- Let's say you are connected via VNC.
  - Login into the OS.
  - Run `pycharm.sh` using terminal (should be there):

```
        > bash /opt/pycharm-community-2021.2.3/bin/pycharm.sh
```

# Installation of VS Code

## Windows, Linux, Mac

- Download the installer using this link:
  [<span class="underline">Download Visual Studio Code - Mac, Linux, Windows</span>](https://code.visualstudio.com/download).
- Run the installer and follow the wizard steps.
- To run VS Code, find it in the **Start** menu or use the desktop shortcut.
- In the left navigation bar search for extensions ( or use `Ctrl+Shift+X` ) and
  search for "ms-vscode-remote.remote-ssh" and then click on the install button.
  <img src="Tools_VisualStudio_Code_figs/image8.png" style="width:3.13386in;height:1.49306in" />
- Connect to the VPN.
- In bottom left corner click on this green button: 
  <img src="Tools_VisualStudio_Code_figs/image3.png" style="width:0.45313in;height:0.41827in" />
- Then you will see these options on top of the screen, click on "Open SSH
  Configuration File…" and then click on the `user\.ssh\config` or
  `user/.ssh/config`.
  <img src="Tools_VisualStudio_Code_figs/image6.png" style="width:4.04688in;height:0.71944in" />
- The config should look like this:
  <img src="Tools_VisualStudio_Code_figs/image7.png" style="width:6.26772in;height:2.59722in" /> 
  - HostName: dev1 (or dev2) server IP 
  - User: your linux user name on the dev
  server 
  - IdentityFile: private key that you use to `SSH` to the dev server
- Save and close the config file and press the green button again, then for
  connection click on "Connect to Host...". You should see the IP address of the
  server, so just click on it and it will connect you in a new window.
- Open a preferred repo directory - Click on the "Source control" button on the
  left
  <img src="Tools_VisualStudio_Code_figs/image4.png" style="width:0.41667in;height:0.48958in" />
  - Choose "Open Folder"
  <img src="Tools_VisualStudio_Code_figs/image1.png" style="width:2.47917in;height:1.59375in" />
  - Choose the desired repo directory from the drop-down menu, e.g., `cmamp1`
  <img src="Tools_VisualStudio_Code_figs/image5.png" style="width:2.5in;height:1.44792in" />
