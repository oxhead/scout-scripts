#!/bin/bash

echo Setup AMI for SCOUT

# get scout codes
SCOUT_DIR=/opt/scout
sudo mkdir $SCOUT_DIR
sudo chmod a+rwx $SCOUT_DIR
cd $SCOUT_DIR
git clone https://github.com/oxhead/scout-scripts.git .

# deploy the scout tools
pip install --editable ${SCOUT_DIR}/sbin
