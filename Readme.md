# Spark Cluster using Raspberry Pi

This repository contains the project template for completing Task 1 and Task 2 of Assignment 3.

> **_NOTE:_**
> After loading the project depending on which IDE (if using), you might see some file not found errors. However, they will disappear after you have successfully built the project using following command from the tasks folder.

## Table of Contents
- [Project Structure](#Project-Structure)
- [Hadoop Setup](#Hadoop-Setup)
- [Setup SSH Passwordless](#Setup-SSH-Passwordless)
- [Spark Setup](#Spark-Setup)
- [Task 1](#Task-1)
- [Task 2](#Task-2)

## Project Structure

```bash
src
│     └── main
│         ├── java
│         │ └── com
│         │     └── assignment3
│         │         └── spark
│         │             ├── CheckOutput.java
│         │             └── WordCount.java


```


## Hadoop Setup

In addition to Spark, in this assignment, you will also use the _Hadoop Distributed File System (HDFS)_ for distributed file storage.


Follow the steps below to set up Hadoop on your machine:
*Note*: _MasterIP_ is the IP address of your local machine. Please update this address if you switch network or devices.
1. Download jdk-8u381 (https://www.oracle.com/java/technologies/downloads/#java8-linux) and install it. Do not set it to the path.
2. Download Hadoop 3.3.1 (https://hadoop.apache.org/release/3.3.1.html) extract and place it in ```c:\``` or equivalent location ```/usr/local/``` choose the location as you want but keep it safe :) .
3. (Not for MacOS/Linux) For Windows users, please extract the bin folder from files-required-hadoop-windows folder and replace the original bin folder of Hadoop.
4. Copy the files from hadoop-config-files folder and replace existing files in the ```etc/hadoop``` folder. Here, please edit ```core-site.xml``` file to add your local machine's IP address instead of MasterIP. Keep this address updated if you change the network or devices.
5. Create two directories outside or in the hadoop folder *\data\dfs\datanode* and *\data\dfs\datanode* and update the *hdfs-site.xml* configuration as follows:

###### Windows
```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>ipc.maximum.data.length</name>
        <value>134217728</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///C:/path/to/data/dfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///C:/path/to/data/dfs/datanode</value>
    </property>
</configuration>

```

###### MacOS
```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>ipc.maximum.data.length</name>
        <value>134217728</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/Users/username/hdfs/data/dfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/Users/username/hdfs/data/dfs/datanode</value>
    </property>
</configuration>
```
5. In the ```etc/hadoop``` folder, locate the ```hadoop-env.sh``` (or ```hadoop-env.cmd``` for Windows) file and update the ```JAVA_HOME``` path to the location of the ```path/to/jdk1.8``` (important for Hadoop to run).
6. Add the following paths and variables into your machine's environment to execute the hadoop commands from your terminal. Note you have to set ```HADOOP_HOME``` to the folder where you installed Hadoop in Step 2.
###### MacOS
```bash
export HADOOP_HOME=/path/to/hadoop-3.3.1/ 
export HADOOP_INSTALL=$HADOOP_HOME 
export HADOOP_MAPRED_HOME=$HADOOP_HOME 
export HADOOP_COMMON_HOME=$HADOOP_HOME 
export HADOOP_HDFS_HOME=$HADOOP_HOME export YARN_HOME=$HADOOP_HOME 
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native 
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin 
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/nativ"
```
###### Windows

set this into the path
```bash
path\to\hadoop-3.1.1\bin 
path\to\hadoop-3.1.1\sbin  
```
7. Now open a terminal (or stay in the same one if you are already there 🙂). Check ```hadoop -version``` or ```hadoop version```, it would show the java version 1.8 or the Hadoop version 3.1.1 that we added to hadoop environment.
8. Please turn on the remote login for your machine (grant all file access).
9. Let's start Hadoop. To do this format the directory that you created for host your files _namenode_,
```bash
hdfs namenode -format
```
Please update the command and run this SH file *path/to/Hadoop/sbin/start-all.sh* based on your OS to host the hadoop file system.
the successful setup would show you following lines in same or multiple (self-starting) terminals:
Starting namenodes
Starting datanodes
Starting secondary namenodes
Starting resourcemanager
Starting nodemanagers

... also a 'native library' or 'c:\program' warning that you may ignore.

To stop the hosting HDFS run *path/to/Hadoop/sbin/stop-all.sh*

Once everything is running. You will be able to access the HDFS hosting on the location *MasterIP:9870*. However, all the insert and delete operations should be performed from a terminal using the following commands:

10. Add our required file to the HDFS (mostly just the input file, but if other files are also not accessible by the worker nodes then they can also be uploaded here -- please update the run commands and the code accordingly in this case)
```bash
hadoop fs -mkdir /sparkApp
hadoop fs -mkdir /sparkApp/input
hadoop fs -put /path/to/pigs.txt /sparkApp/input
```
Please double-check the stored input file on the *MasterIP:9870* under utility tab *browse the file system*

11. To remove folders from the HDFS, execute following command
```bash
hdfs dfs -rm -r /path/to/folder
```


## Setup SSH Passwordless
For Linux this tutorial https://scott-ralph.medium.com/creating-an-apache-spark-cluster-with-raspberry-pi-workers-472c2c9a19ce might help.
For MacOS this tutorial https://medium.com/@anirudhtulasi.x/how-to-set-up-password-less-ssh-for-localhost-on-macos-e54fff167169 might help.
For Windows 11, here are some steps (and https://winscp.net/eng/docs/guide_windows_openssh_server might help.):
1. Go to Settings > Apps > Optional features and click on View features.
2. Locate “OpenSSH server” feature, select it, click Next, and then click Install.
3. Open the Services desktop app. (Select Start, type services.msc in the search box, and then select the Service app or press ENTER.)
4. In the details pane, double-click OpenSSH SSH Server.
5. On the General tab, from the Startup type drop-down menu, select Automatic.
6. To start the service, select Start.
7. Create a new file called authorized keys in "c:\Users\USER\.ssh\authorized_keys"
8. Copy the contents of your public key into this file.
9. Uncomment the line "PubkeyAuthentication yes" on this file "C:\ProgramData\ssh\sshd_config"

## Spark Setup

*Note* Please check your JAVA_HOME and set Java 17 as JAVA_HOME in your environment, if it is not already set. 

1. Download spark-3.5.0-bin-hadoop3 from [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html) and extract the spark directory containing bin and sbin folders
2. Set the path,
###### On MacOS:
```bash 
   nano ~/.zshrc
``` 
Add the following paths to the shell and save it.

```bash 
   #Apache-spark
   export PATH="$PATH:/path/to/spark-3.5.0-bin-hadoop3/bin"
   export PATH="$PATH:/path/to/spark-3.5.0-bin-hadoop3/sbin"
```

3. Check the Spark-Shell working
   run ```spark-shell``` in the terminal/shell.
   Note: go to the folder: usr/local/spark/bin and run it there!!!!
   ```bash 
   cd /usr/local/spark/bin
   ```

it should give you a link where spark UI is hosted, example,  ht`<span>`tp://MasterIP:4040 and it will allow you to interact with it using scala. To exit from the shell enter ``:quit`` .

Please go ahead and look at the UI through a web browser on your local machine. The resulting page would show you job trace in the default tab 'Job'.

![initial spark UI](https://github.com/Interactions-HSG/BCS-DS-Asignments/blob/0167674a989b4b03ec36ab54dd804da90a66b547/Assignment3/spark_init_ui.jpg)

Command to get IP address:
```bash
ipconfig getifaddr en0
```
Command to send from mac to dani:
From mac terminal without raspi connections:
```bash
scp /Users/luisamuller/quicksort.py luisamuller@172.20.10.2:assignment3
```
note that 172.20.10.2 is the danis IP address and /Users/luisamuller/quicksort.py is the place where it is stored on my mac.

## Task 1
1. Run the application from tasks folder
###### On MacOS:

First run the command:
```bash
export PATH=$PATH:/usr/local/spark/bin/
```

```bash
./gradlew build

spark-submit --class com.assignment3.spark.WordCount  app/build/libs/app.jar
```

###### On Windows:
```bash
.\gradlew build
spark-submit --class com.assignment3.spark.WordCount  app/build/libs/app.jar
```
2. Test your output (optional)
```bash
./gradlew build
./gradlew run -PchooseMain=com.assignment3.spark.CheckOutput --args="path/to/given/output.txt /path/to/your/output.txt"
```

###### On Windows:
```bash
.\gradlew build
.\gradlew run -PchooseMain="com.assignment3.spark.CheckOutput" --args="path/to/given/output.txt /path/to/your/output.txt"
```

## Task 2

#### Create a single node Spark cluster using Raspberry Pi

1. Format Raspberry Pi with Raspberry Pi OS (32 bit) Released 03-05-2023 or later. 
   During the setup, please make sure the ***username of the RPi is same as the username on your local machine***. It is important for the workers in order to make request on _just_ the given IP address.

##### Setup Raspberry Pi (Worker)
<details>
  <summary>Click me</summary>

Follow the following steps to install Raspberry Pi OS to your Pi.

### 1. Burn the OS image to the SD card using Raspberry Pi Imager

Download and install [Raspberry Pi Imager](https://rptl.io/imager) for your computer and run it.
Be sure to select "Raspberry Pi OS (32-bit): A port of Debian bullseye with the Raspberry Pi Desktop (Recommended)" since Raspberry Pi 400 requires the video driver.

### 2. Prepare the configuration files in the Task1 directory to the SD card

On the newly flashed SD card, we need to put some additional initial configuration files to prepare a headless Raspberry Pi – "headless" means that the OS does not expect/require a connected monitor or keyboard.

Find the following files in the `rpi-user-config-files` directory:

- `ssh`: This file enables an [SSH](https://en.wikipedia.org/wiki/Secure_Shell) server on the Pi by placing this file **in the boot folder**. You will use SSH to connect to the Pi through the network, i.e., using your own computer as an SSH client. See the [Enabling the Server](https://www.raspberrypi.com/documentation/computers/remote-access.html#enabling-the-server) section of the official doc for details. You don't have to do anything about this file.
- `userconf.txt`: This file configures the initial user on the Pi **at the initial boot**. You _should_ update this file.
- `wpa_supplicant.conf`: This file configures your Pi to connect to a Wi-Fi network **at the initial boot**. You _should_ update this file, and you will need to update it to connect to alternative networks (e.g., your home network or a hotspot on your mobile phone).

**By copying these file to the `boot` partition of the SD card you flashed in Step 1, your Pi will automatically connect to the Wi-Fi SSID:labnet and create a user called `ics` with a password `mypassword`.**

We recommend the following changes:

#### Use your own username

The default configuration in the `userconf.txt` creates the user `ics` with encrypted password `mypassword`, but you may want to change the username as you like.
For this, simply change the `ics` to a username same as your local machine for easiness.
We refer to the username (even if you didn't change) as `$USER` for the rest of these instructions.

You could also update the password, but we recommend you to do so after you gain access to the Pi for simplicity.

Refer to the [Configuring a User](https://www.raspberrypi.com/documentation/computers/configuration.html#configuring-a-user) section of the official doc for details, if you want more information on it.

#### Communicate with your Pi wirelessly over your mobile phone

For several reasons, we recommend you to use your phone as a hotspot and connect your Pi to it.
First of all, find the instructions on how to share the network connection on your phone and set up your own hotspot. Take note of the SSID and the password you configured.

- Android: https://support.google.com/android/answer/9059108?hl=en
- iPhone: https://support.apple.com/en-us/HT204023

For the Pi to connect to your phone's network, edit the file `wpa_supplicant.conf` to configure the SSID (in place of "`labnet`") and the password (in place of "`inthrustwetrust`").

If you are interested, refer to the [Setting up a Headless Raspberry Pi](https://www.raspberrypi.com/documentation/computers/configuration.html#setting-up-a-headless-raspberry-pi) section in the official doc for more details.

### 3. Find your Pi in the Wi-Fi network

Once you finished Step 2 and power on the Pi with the SD card, it should automatically connect to the configured Wi-Fi network.
To connect your machine to the Pi through the network, you first have to find the IP address of the Pi.

If you have connected the device to your mobile phone, you may check your phone's hotspot's currently connected devices to find the Pi's IP address. In this case you may already advance to Step 4. In any case, however, you will be able to find the Pi's IP address in the following way:

Connect your computer to the same Wi-Fi as the Pi.

MAC addresses of the devices connected to a network can be obtained by using the [Address Resolution Protocol (ARP)](https://en.wikipedia.org/wiki/Address_Resolution_Protocol).
Concretely, run the following command from the console on your computer.

For both Terminal on macOS and PowerShell on Windows:

```sh
arp -a
```

The Wi-Fi network interface card (NIC) of a Raspberry Pi 400 has a specific prefix like "`E4:5F:01:xx:xx:xx`" in its [MAC address](https://en.wikipedia.org/wiki/MAC_address) – this is to verify the device is from [Raspberry Pi Foundation](https://udger.com/resources/mac-address-vendor-detail?name=raspberry_pi_foundation). Hence, in your arp output, find the IP address associated with a MAC address starts with `E4:5F:01`.

We refer to this IP address as `$IP` for the rest of this instruction.

### 4. SSH to the Pi with the password

The next step is to access the Pi via SSH.
Open the console on your computer and type the following command (remember to replace the values `$IP` and `$USER`):

```sh
ssh $IP -l $USER
```

When you see a message like below, you have successfully logged in the Pi via SSH.

```console
Linux raspberrypi 5.15.61-v7l+ #1579 SMP Fri Aug 26 11:13:03 BST 2022 armv7l

The programs included with the Debian GNU/Linux system are free software;
the exact distribution terms for each program are described in the
individual files in /usr/share/doc/*/copyright.

Debian GNU/Linux comes with ABSOLUTELY NO WARRANTY, to the extent
permitted by applicable law.
Last login: Tue Nov 29 09:50:40 2022 from $IP
$USER@raspberrypi:~ $ 
```

### 5. Learn how to SCP (optional)

The last step is to learn how to copy files from your computer to the Pi.
The most convenient way to do this is to use the secure-copy tool [SCP](https://www.ssh.com/academy/ssh/scp).

For example, the following command will copy the file `main.c` on your computer to the `$USER` user's home directory.

```sh
scp main.c $USER@IP:main.c
```

and the following will copy the file from the Pi to your computer.

```sh
scp $USER@IP:main.c main.c
```

### 6. Set up public key authentication for password-less communication between the master and the worker
you can set up a public key authentication to save some time to type the password each time you need to SSH to the Pi. Knowing how to set up key-based authentication is a valuable skill that you will need many times in the years to come - hence, go for this! To do it, follow this guide: https://www.ssh.com/academy/ssh/authorized-keys-openssh
or 
Follow this tutorial https://www.strongdm.com/blog/ssh-passwordless-login

You can optionally assign static IP to your Raspberry Pi if you switch the devices or networks often, https://raspberrypi-guide.github.io/networking/set-up-static-ip-address

</details>

#### Setup Spark on the Raspberrypi

2. Download spark-3.5.0-bin-hadoop3 from [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html). Or use the previously downloaded file.
3. Connect to your Raspberry Pi through SSH
4. From another terminal of your local machine, copy spark-3.5.0-bin-hadoop3.tgz to your Raspberry Pi --- this process is similar as you did on your local machine

```bash
scp path/to/the/file username@ip:path/to/destination/folder
```

5. Coming back to the RPi terminal, extract the Spark installation package

```bash
tar vxf ~/Downloads/spark-3.4.1-bin-hadoop3.tgz
```

6. Set the path to the bin folder of the spark package

```bash
nano ~/.bashrc
```
add following paths
```bash
export PATH="$PATH:/path/to/spark-3.4.1-bin-hadoop3/bin"
export PATH="$PATH:/path/to/spark-3.4.1-bin-hadoop3/sbin"
```

7. Install Java 17 on Raspberry Pi (Spark supports this version better than the newer Java 21)

```bash
sudo apt-get install openjdk-17-jdk -y
```

8. Check your Java version, it should be 17.0.* 

```bash
java --version
```

9. Run Spark on Raspberry Pi

```bash
spark-shell
```

Use absolut path to direct to spark shell:
```bash
/usr/local/spark/bin/spark-shell
```

it should give you a link where spark UI is hosted, example,  ht`<span>`tp://url:4040 and it will allow you to interact with it using scala. To exit from the shell enter ``:quit`` .

Please go ahead and look at the UI through a web browser on your local machine. The resulting page would show you job trace in the default tab 'Job'.

![initial spark shell UI](https://github.com/Interactions-HSG/BCS-DS-Asignments/blob/0167674a989b4b03ec36ab54dd804da90a66b547/Assignment3/spark_init_ui.jpg)

#### Modify and run the Spark environment on your local machine (Master) ***Note*** step 11 is only for MacOS or Linux
11 a. Add workers to the environment **for MacOS**
 convert the workers.template file to workers file (by using notepad/other text editors on *Windows*)
###### MacOS
```bash
   nano /path/to/spark-3.5.0-bin-hadoop3/conf/workers.template
```
 add the ip address (example: 172.20.10.12) at the end of the file
 save the file and change the name to save it as workers in the conf directory


11 b. Add master to the environment **for MacOS**
  convert the spark-env.sh.template file to spark-env.sh file (by using notepad/other text editors on *Windows*)
###### MacOS
```bash
nano /path/to/spark-3.5.0-bin-hadoop3/conf/spark-env.sh
```
add 
```bash
export SPARK_MASTER_HOST="local_machine_IP (MasterIP)"
```
at the end of the file and save the file by changing its name to spark-env.sh in the conf directory

12. Start the Spark environment

###### MacOS
```bash
start-all.sh
```
to Stop all the masters and the workers (after you are done executing the task)
```bash
stop-all.sh
```
###### Windows
start the cluster in a separate terminal
```bash
spark-class org.apache.spark.deploy.master.Master --host <IP_ADDR>
```
for each worker --- for example 2 workers in separate terminals:
```bash
spark-class org.apache.spark.deploy.worker.Worker spark://MasterIP:MasterPort --host <IP_ADDR>
spark-class org.apache.spark.deploy.worker.Worker spark://MasterIP:MasterPort --host <IP_ADDR>
```
**Note** The password less access must exist between the master and all the workers (RPi, Local machine (localhost)). If you are getting permission or unable to bind errors in running the workers, check if you can do ```ssh localhost``` on your machine.

13.Find the master url and port by looking at the master UI page on url:8080 (example: 172.20.10.14:8080). It should now show you a list of alive workers. An example screenshot is in the Assignment Sheet.
14. For this assignment we are using hadoop to store the required files for all the cluster workers. Therefore, to help the clusters find the required files, upload the input file and use the url of the file as ```hdfs://masterIP:9000/path/to/inputfile``` same for creating and saving output on the hdfs
15. While the Spark environment is running, use the following command to execute the Word count application on the cluster from the tasks folder (update your code and the command with the master IP and Port):
###### MacOS
```bash
 ./gradlew build
 spark-submit --class com.assignment3.spark.WordCount app/build/libs/app.jar --deploy-mode cluster
 ```
###### Windows
```bash
 .\gradlew build
 spark-submit --class com.assignment3.spark.WordCount app/build/libs/app.jar --deploy-mode cluster
 ```
Please delete the output folder if needed using the *rm* command given in Hadoop installation section.
