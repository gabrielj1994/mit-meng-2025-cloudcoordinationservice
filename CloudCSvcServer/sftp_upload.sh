#!/bin/bash

sftp -i "AWS_ZK_KP.pem" ec2-user@ec2-44-203-80-13.compute-1.amazonaws.com <<EOF
put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/CloudCSvcServer/target/CloudCSvcServer-1.0-SNAPSHOT-jar-with-dependencies.jar
put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/CloudCSvcApplicationTests/target/CloudCSvcApplicationTests-1.0-SNAPSHOT-jar-with-dependencies.jar
exit
EOF

sftp -i "AWS_ZK_KP.pem" ec2-user@ec2-98-80-196-196.compute-1.amazonaws.com <<EOF
put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/CloudCSvcServer/target/CloudCSvcServer-1.0-SNAPSHOT-jar-with-dependencies.jar
put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/CloudCSvcApplicationTests/target/CloudCSvcApplicationTests-1.0-SNAPSHOT-jar-with-dependencies.jar
exit
EOF

sftp -i "AWS_ZK_KP.pem" ec2-user@ec2-13-219-254-45.compute-1.amazonaws.com <<EOF
put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/CloudCSvcServer/target/CloudCSvcServer-1.0-SNAPSHOT-jar-with-dependencies.jar
put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/CloudCSvcApplicationTests/target/CloudCSvcApplicationTests-1.0-SNAPSHOT-jar-with-dependencies.jar
exit
EOF

############################## OLD
#sftp -i "AWS_ZK_KP.pem" ec2-user@ec2-44-202-250-29.compute-1.amazonaws.com <<EOF
#sftp -i "AWS_ZK_KP.pem" ec2-user@ec2-44-213-112-44.compute-1.amazonaws.com <<EOF
#sftp -i "AWS_ZK_KP.pem" ec2-user@ec2-44-203-80-13.compute-1.amazonaws.com <<EOF
#put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/ZKServer/target/ZKServer-1.0-SNAPSHOT-jar-with-dependencies.jar
#put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/ZKTestApplication/target/ZKTestApplication-1.0-SNAPSHOT-jar-with-dependencies.jar
#exit
#EOF
#
#sftp -i "AWS_ZK_KP.pem" ec2-user@ec2-98-80-196-196.compute-1.amazonaws.com <<EOF
#put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/ZKServer/target/ZKServer-1.0-SNAPSHOT-jar-with-dependencies.jar
#put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/ZKTestApplication/target/ZKTestApplication-1.0-SNAPSHOT-jar-with-dependencies.jar
#exit
#EOF
#
#sftp -i "AWS_ZK_KP.pem" ec2-user@ec2-13-219-254-45.compute-1.amazonaws.com <<EOF
#put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/ZKServer/target/ZKServer-1.0-SNAPSHOT-jar-with-dependencies.jar
#put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/ZKTestApplication/target/ZKTestApplication-1.0-SNAPSHOT-jar-with-dependencies.jar
#exit
#EOF
#
##sftp -i "AWS_ZK_KP.pem" ec2-user@ec2-44-203-16-254.compute-1.amazonaws.com <<EOF
#sftp -i "AWS_ZK_KP.pem" ec2-user@ec2-3-239-233-102.compute-1.amazonaws.com <<EOF
#put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/ZKServer/target/ZKServer-1.0-SNAPSHOT-jar-with-dependencies.jar
#put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/ZKTestApplication/target/ZKTestApplication-1.0-SNAPSHOT-jar-with-dependencies.jar
#exit
#EOF
#
#sftp -i "AWS_ZK_KP.pem" ec2-user@ec2-3-237-170-207.compute-1.amazonaws.com <<EOF
#put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/ZKServer/target/ZKServer-1.0-SNAPSHOT-jar-with-dependencies.jar
#put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/ZKTestApplication/target/ZKTestApplication-1.0-SNAPSHOT-jar-with-dependencies.jar
#exit
#EOF
#
#sftp -i "AWS_ZK_KP.pem" ec2-user@ec2-3-239-29-75.compute-1.amazonaws.com <<EOF
#put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/ZKServer/target/ZKServer-1.0-SNAPSHOT-jar-with-dependencies.jar
#put -f /Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/ZKTestApplication/target/ZKTestApplication-1.0-SNAPSHOT-jar-with-dependencies.jar
#exit
#EOF
