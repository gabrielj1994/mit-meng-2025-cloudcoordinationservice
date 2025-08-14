sftp ubuntu@ec2-100-27-34-135.compute-1.amazonaws.com
#put -f /Users/gabrieljimenez/GIT/MIT-ZooKeeper-Project/ZKServer/target/ZKServer-1.0-SNAPSHOT.jar
put -f /Users/gabrieljimenez/GIT/MIT-ZooKeeper-Project/ZKServer/target/ZKServer-1.0-SNAPSHOT-jar-with-dependencies.jar
#exit
sftp ubuntu@ec2-44-192-116-184.compute-1.amazonaws.com
#put -f /Users/gabrieljimenez/GIT/MIT-ZooKeeper-Project/ZKServer/target/ZKServer-1.0-SNAPSHOT.jar
put -f /Users/gabrieljimenez/GIT/MIT-ZooKeeper-Project/ZKServer/target/ZKServer-1.0-SNAPSHOT-jar-with-dependencies.jar
#exit

#java -cp ZKServer-1.0-SNAPSHOT-jar-with-dependencies.jar dsg.tbd.SandboxCLI ec2-100-27-34-135.compute-1.amazonaws.com 8001 benchmark /test1

##
#  ssh -i "AWS_ZK_KP.pem" ec2-user@ec2-44-198-61-232.compute-1.amazonaws.com
#  ssh -i "AWS_ZK_KP.pem" ec2-user@ec2-18-214-40-132.compute-1.amazonaws.com
sftp ec2-user@ec2-44-198-61-232.compute-1.amazonaws.com