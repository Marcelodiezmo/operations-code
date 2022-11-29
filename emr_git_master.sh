#!/bin/bash

sudo yum install git-core -y

git clone -b master https://BernardoH:yhzMR3MTrBJ7kacuwjRG@bitbucket.org/techubits/skynetcode.git

sudo rm -rf ./skynetcode/.git

sudo rm ./skynetcode/.gitignore

sudo aws s3 rm s3://analytics-ubits-production/git/skynetcode --recursive 

sudo aws s3 cp ./skynetcode s3://analytics-ubits-production/git/skynetcode/ --recursive 


