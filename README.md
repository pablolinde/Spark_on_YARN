# Spark_on_YARN
This repository contains a NASA meteorite dataset processed using Spark on YARN within a dockerized, simulated Hadoop cluster. If you want to perform your own analysis on a dataset of your choice, follow the instructions below.

## How to use:
- Download the virtual machine with Spark and the Hadoop cluster already installed and configured using the following link: [Spark_on_YARN.ova](https://drive.google.com/file/d/1xlUZP1TBtR94wRlprNRMVCWWGajC_APO/view?usp=drive_link)

- Start/stop the cluster when starting/ending a session:

docker container start namenode datanode{1..4}  
docker container stop namenode datanode{1..4}


- Download the dataset (in this example meteorites.csv) on the namenode, and upload it to HDFS:
  
docker container exec -ti namenode /bin/bash    
su - hdadmin    
wget -O meteorites.csv "https://data.nasa.gov/api/views/gh4g-9sfh/rows.csv?accessType=DOWNLOAD"  
hdfs dfs -mkdir -p /user/luser/datasets  
hdfs dfs -put meteorites.csv /user/luser/datasets/  
hdfs dfs -chown -R luser /user/luser  
exit  
exit  

- Install dependencies if necessary (create and execute on the host):
  
nano install_deps.sh  
chmod +x install_deps.sh  
./install_deps.sh  

- Enter the namenode again as luser:
 
docker container exec -ti namenode /bin/bash  
su - luser  

- Create, save the PySpark script in a file (in this example meteorites.py), then execute it:

nano meteorites.py  
spark-submit --master yarn meteorites.py   

(If you want to see a cleaner output, add 2>&1 | grep -vE "INFO|WARN" at the end of the execution command).     
