Rename .env_sample to .env and fill in your bitquery API_KEY to run

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


####################################################
## Running project
####################################################
go to project directory
cd ~\sorrentum_sandbox\devops
docker-compose up -d
./init_airflow_setup.sh
./docker_bash.sh

docker> cd /cmamp/sorrentum_sandbox/examples/ml_projects/Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap
docker> python3 download_to_csv.py --start_timestamp '2023-04-20T16:38:00' --target_dir 'uniswap_data'
docker> python3 download_to_db.py --start_timestamp '2023-04-20T16:38:00' --target_table 'uniswap_table'
docker> python3 download_to_db.py --start_timestamp '2023-04-20T16:38:00' --target_table 'uniswap_table' -live_flag


####################################################
## Running project in Windows
####################################################
For windows users from project directory run:
go to project directory
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
docker> python3 download_to_db.py --start_timestamp '2023-04-20T16:38:00' --target_table 'uniswap_table' --live_flag




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

