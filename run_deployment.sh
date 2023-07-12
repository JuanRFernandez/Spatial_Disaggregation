#!/bin/bash
set -e # exits upon first error in any of the commands

ENV_NAME=$1
DJANGO_CACHE_LOCATION=$2

# update the system
sudo apt-get update && sudo apt-get upgrade --yes
sudo apt clean
sudo apt autoremove

#Since conda is a bash function and bash functions can not be propagated 
#to independent shells (e.g. opened by executing a bash script), one has to add the line below
source ~/anaconda3/etc/profile.d/conda.sh

# cd into the repository
cd ~/code/spatial_disaggregation/

# discard local changes (if any) and pull new version of the code 
git reset --hard 
#TODO: check if there's a safer way to discard changes. 
#"restore" work for 2.23+ versions. cant update git on debian for some reason
git pull origin master 

# update the environment
mamba env update -n $ENV_NAME --file=requirements.yml

# activate the environment
conda activate $ENV_NAME

# install builda
cd ../builda-client/
git checkout v4.0
pip install -e.

# install geokit
cd ../geokit/
pip install -e. 

# populate new DB  
cd ../spatial_disaggregation/
bash run_db_population.sh $DJANGO_CACHE_LOCATION

# restart gunicorn and nginx 
sudo systemctl restart gunicorn
sudo systemctl restart nginx

