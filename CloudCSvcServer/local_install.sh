rm -rf ~/.m2/repository/dsg/tbd/CloudCSvcServer
mvn install:install-file \
-Dfile=/Users/gabrieljimenez/GIT/MIT-CoordinationService-Project/CloudCSvcServer/CloudCSvcServer-1.0-SNAPSHOT-jar-with-dependencies.jar \
-DgroupId=dsg.ccsvc \
-DartifactId=CloudCSvcServer \
-Dversion=1.0-SNAPSHOT \
-Dpackaging=jar \
-DgeneratePom=true
