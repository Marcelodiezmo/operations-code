#!/bin/bash

sudo yum install git-core -y

git clone -b develop https://BernardoH:yhzMR3MTrBJ7kacuwjRG@bitbucket.org/techubits/opereaciones.git

sudo rm -rf ./operaciones/.git

sudo rm ./operaciones/.gitignore

sudo aws s3 rm s3://strategysc/git/operaciones --recursive 

sudo aws s3 cp ./operaciones s3://strategysc/git/operaciones/ --recursive 

