###################################################
## Container info
####################################################

Airflow Info:
Username: airflow
Password: airflow

Postgres Info:
Username: postgres
Password: postgres
Port: 5532
host: host.docker.internal
database: airflow


####################################################
## Running project - Startup Airflow
####################################################

## In $GIT_ROOT/sorrentum_sandbox/examples/ml_projects/Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap:
## Rename .env_sample to .env and fill in your bitquery API_KEY

## go to sorrentum_sandbox directory
cd ~\sorrentum_sandbox\devops
docker-compose up -d
./init_airflow_setup.sh
./docker_bash.sh

docker> cd /cmamp/sorrentum_sandbox/examples/ml_projects/Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap
docker> python3 download_to_csv.py --start_timestamp '2023-04-20T16:38:00' --target_dir 'uniswap_data'
docker> python3 download_to_db.py --start_timestamp '2023-04-20T16:38:00' --target_table 'uniswap_table'

## Login to airflow and start the download_periodic_12hr_postgres_uniswap DAG and uniswap_1h_analysis_calculation (This runs Dask_script.py):
http://localhost:8091/home

####################################################
## Running jupyter notebook
####################################################
cd $GIT_ROOT/sorrentum_sandbox/examples/ml_projects/Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap
./docker_jupyter.sh

## Jupiter notebook location: (Paste into browser)
http://localhost:8888/notebooks/data/sorrentum_sandbox/examples/ml_projects/Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap/uniswap_notebook.ipynb




####################################################
## Running project - Startup Airflow in Windows
####################################################
# To run in windows download the Ubuntu-20.04 wsl package from the microsoft store
# after installing open up powershell and run the following commands:
# wsl --set-default Ubuntu-20.04
# wsl
# sudo apt update && upgrade
# sudo apt install python3 python3-pip ipython3
# sudo apt install dos2unix
## In $GIT_ROOT/sorrentum_sandbox/examples/ml_projects/Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap:
## Rename .env_sample to .env and fill in your bitquery API_KEY

## go to sorrentum_sandbox directory
cd ~\sorrentum_sandbox\devops
docker-compose up -d
wsl
dos2unix init_airflow_setup.sh
./init_airflow_setup.sh
dos2unix docker_bash.sh
./docker_bash.sh

docker> cd /cmamp/sorrentum_sandbox/examples/ml_projects/Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap
docker> python3 download_to_csv.py --start_timestamp '2023-04-20T16:38:00' --target_dir 'uniswap_data'
docker> python3 download_to_db.py --start_timestamp '2023-04-20T16:38:00' --target_table 'uniswap_table'

## Login to airflow and start the download_periodic_12hr_postgres_uniswap DAG and uniswap_1h_analysis_calculation (This runs Dask_script.py):
http://localhost:8091/home

####################################################
## Running jupyter notebook in Windows
####################################################
cd ~\sorrentum_sandbox\examples
wsl
dos2unix docker_jupyter.sh set_env.sh run_jupyter.sh
./docker_jupyter.sh

## Jupiter notebook location: (Paste into a web-browser)
http://localhost:8888/tree/sorrentum_sandbox/examples/ml_projects/Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap

=======
Rename .env_sample to .env and fill in your bitquery API_KEY to run

### OLD Remove if not needed later ###
Instructions for creating the dockerfile:
docker build -t data605 .
docker run -p 8888:8888 -p 5432:5432 -p 8080:8080 --name data605_container -d data605
#################



###################################################
## Container login info
####################################################

Airflow login:
Username: airflow
Password: airflow

Postgres airflow username and password:
Username: postgres
Password: postgres
database: airflow


###################################################
## Build Image / Container
####################################################


go to 
cd ~\sorrentum_sandbox\devops
docker-compose up -d

####################################################
## Running project
####################################################
cd sorrentum_sandbox\examples\ml_projects\Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap/
unix2dos docker_bash.sh
./docker_bash.sh

docker> cd /cmamp/sorrentum_sandbox/examples/ml_projects/Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap/
docker> python3 download_to_csv.py --start_timestamp '2023-04-20T16:38:00' --target_dir 'uniswap_data'
docker> python3 download_to_db.py --start_timestamp '2023-04-20T16:38:00' --target_table 'uniswap'

####################################################
## Running project in Windows
####################################################
For windows users from project directory run:
wsl
dos2unix docker_bash.sh
./docker_bash.sh

docker> cd /cmamp/sorrentum_sandbox/examples/ml_projects/Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap/
docker> python3 download_to_csv.py --start_timestamp '2023-04-20T16:38:00' --target_dir 'uniswap_data'
docker> python3 download_to_db.py --start_timestamp '2023-04-20T16:38:00' --target_table 'uniswap'
>>>>>>> master


###################################################
## postgres login and basic commands
####################################################
# in postgres container
psql -h localhost -U postgres airflow

# load table names
\dt 

# describe table
\d <tablename> 

# delete table
DROP TABLE <tablename>

<<<<<<< HEAD
###################################################
## Jupyter
####################################################

In outside terminal 
> cd $GIT_ROOT/sorrentum_sandbox/examples
> ./docker_jupyter.sh

In local host, navigate to our folder 
might have to pip install psycopg2