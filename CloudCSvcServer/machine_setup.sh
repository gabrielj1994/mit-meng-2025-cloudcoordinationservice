#!/bin/bash
mkdir /home/ec2-user/WORKSPACE
cat > /home/ec2-user/WORKSPACE/psql_cmd.txt << EOF
psql \
   --host=csvc-db-2-instance-1.c7oym4pnfncu.us-east-1.rds.amazonaws.com \
   --port=5432 \
   --username=csvcadmin \
   --password \
   --dbname=postgres
EOF

cat > /home/ec2-user/zoo_replicated.cfg << EOF
tickTime=2000
dataDir=/home/ec2-user/zookeeper
clientPort=2181
initLimit=5
syncLimit=2
server.1=ec2-3-239-27-218.compute-1.amazonaws.com:2888:3888
server.2=ec2-44-203-51-142.compute-1.amazonaws.com:2888:3888
server.3=ec2-44-201-37-35.compute-1.amazonaws.com:2888:3888
EOF

sudo yum -y update && sudo yum upgrade -y
sudo yum -y install java-1.8.0-amazon-corretto
sudo yum -y install postgresql15

#sftp -i "AWS_ZK_KP.pem" ec2-user@<hostname> <<EOF
#put -rf /Users/gabrieljimenez/WORKSPACE/apache-zookeeper-3.7.1-bin
#exit
#EOF
