#!/usr/bin/env bash

# Create Virtual Environment for application to run

TARGET=/home/ubuntu/pyzero_dtq
rm -rf ${TARGET}/.venv
python3 -m venv ${TARGET}/.venv
source ${TARGET}/.venv/bin/activate
pip install -r ${TARGET}/requirements.txt
deactivate


