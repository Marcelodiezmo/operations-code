#!/bin/bash

sudo aws s3 cp s3://analytics-ubits-production/git/skynetcode/prod/operaciones/scripts/in_py_dimensiones_sheets_to_s3.py in_py_dimensiones_sheets_to_s3.py

sudo docker cp in_py_dimensiones_sheets_to_s3.py jupyterhub:/home/jovyan/in_py_dimensiones_sheets_to_s3.py

sudo docker exec jupyterhub python3 in_py_dimensiones_sheets_to_s3.py



