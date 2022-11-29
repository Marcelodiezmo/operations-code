#!/bin/bash

cd /usr/lib/spark/jars/
sudo aws --debug --region us-east-1 s3 cp s3://analytics-ubits-production/emr-config/mysql-connector-java-8.0.17.jar

sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh

sudo docker exec jupyterhub pip install boto3 colorama statsmodels nltk prince unidecode pandas scikit-learn fastparquet pyarrow tzlocal msal pymongo bson

sudo docker exec jupyterhub pip install scikit-learn boto3 --upgrade

sudo git clone https://mrodriguezubits:R23vjebprdKRfBmE657p@bitbucket.org/techubits/skynetmodule.git

pwd

ls

cd /usr/lib/spark/jars/skynetmodule

sudo python3 -m pip install wheel

sudo python3 -m pip install pathlib

sudo python3 -m pip install setuptools

sudo python3 ./setup.py bdist_wheel

pwd

ls

sudo docker cp /usr/lib/spark/jars/skynetmodule/dist/skynetmodule-0.1.0-py3-none-any.whl jupyterhub:/home/jovyan/skynetmodule-0.1.0-py3-none-any.whl

sudo docker exec jupyterhub pip install --user oauth2client

sudo docker exec jupyterhub pip install ./skynetmodule-0.1.0-py3-none-any.whl

sudo aws s3 cp /usr/lib/spark/jars/skynetmodule/dist/skynetmodule-0.1.0-py3-none-any.whl s3://analytics-ubits-production/emr-config/skynetmodule-0.1.0-py3-none-any.whl

sudo docker exec jupyterhub chown -R jovyan:root /home/jovyan