Dockerization of Hbase is not suitable for the project and hinders the background
processes of the Apache applications. Due to these limitations, we will be installing and
downloading the applications for HBase and Phoenix.

**Installing Hbase**
-----------------------
We will be using Hbase 2.5.8, so install Hbase using this link
https://hbase.apache.org/downloads.html
  - Make sure to install the bin.tar.gz version of Hbase
  - Make sure that we are installing the 2.5.8 version

**Hbase Set-Up**
-------------------------
Now that you have your Hbase bin file downloaded, we are going to edit some files

1) Using any editor you like open ~/.bashrc.
2) Insert the following to the very bottom of your ~/.bashrc file

export HBASE_HOME=full_path_to_hbase_file
export PATH=$PATH:$HBASE_HOME/bin

  - Make sure to fill in the export HBASE_HOME with your path to the hbase file


4) Save and exit the ~/.bashrc file
5) Go into the hbase file, and go into conf, and edit the hbase-env.sh file using any
   editor you like. After opening up add this line under the "The jave implementation to use"

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
  - This requires to have java 11 installed, using the apt install option

5) Save the hbase-env.sh file and exit
6) Go back into the hbase file, and go into conf, and edit the hbase-site.xml file.
   You will be adding the lines of code from the Copy_Paste.txt.

  - Make sure to add it in between the configuration tags.
  - Make sure NOT to place it in between the already existing property tags.
  - Make sure to fill into the path of your hbase file where the line requires it

7) Save the file and exit
8) You are done with the setup

**Installing Phoenix**
------------------------
Now install the Apache Phoenix file using the link

https://phoenix.apache.org/download.html

We are going to be using Phoenix 5.2.0. 
  - Here install the hbase-2.5-bin. 
  - Make sure to untar the file


**Phoenix Set-Up**
--------------------------
1) Go into the phoenix folder
2) Copy this file phoenix-server-hbase-2.5-5.2.0.jar into your
   hbase/lib folder
3) Go into the hbase/conf folder and edit the hbase-site.xml
   Add in the lines of code from the Copy_Paste.txt

   - Make sure to add it in between the configuration tags
   - Make sure to NOT place it in between the already existing property tags

4) You are good to go
