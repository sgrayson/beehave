#!/bin/bash

./pants compile beehave-app/src/main/scala/com/beehave/api
rm dist/server.jar
./pants binary beehave-app/src/main/scala/com/beehave/api:bin
scp -i ~/Downloads/batman.pem dist/server.jar ubuntu@ec2-52-27-137-236.us-west-2.compute.amazonaws.com:~
