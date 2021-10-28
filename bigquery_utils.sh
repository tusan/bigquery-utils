#!/bin/bash
if [ ! -d $PWD/venv/ ]
then
    echo "Error: virtualenv does not exists. The requirements will be installed soon"
    virtualenv --python=python3 venv
    source $PWD/venv/bin/activate
    pip3 install -r requirements.txt
fi
source $PWD/venv/bin/activate
python3 main.py $1 $2 $3 $4 $5 $6 $7