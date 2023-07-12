#!/bin/bash
set -e # exits upon first error in any of the commands

# deprecate previous DB and create a new one 
cd zoomin/database/
python db_deprecation.py
python db_creation.py

# make django migrations to create tables 
python ../api/manage.py makemigrations v1_api 
python ../api/manage.py migrate 

# collect static files 
python ../api/manage.py collectstatic --noinput

# populate the DB tables 
cd ../../notebooks/data_processing
jupyter nbconvert --to python *.ipynb

start=`date +%s`

#1. meta files: tags, independent tables, shapefiles, data sources  
## collect files 
exec_files_1=()

for i in {1..5}; 
do  
    _exec_files=`find . -name $i'_*.py'`
    exec_files_1+=(${_exec_files[@]})
done

## execute files 
for i in ${exec_files_1[@]}; 
do
    echo "executing file: $i -------------------------------"
    python $i
    echo "done ---------------------------------------------"
done

end=`date +%s`

runtime=$((end-start))
echo "time taken: $runtime s"

# clear cache
DJANGO_CACHE_LOCATION=$1
cd $DJANGO_CACHE_LOCATION 
rm -f * 
