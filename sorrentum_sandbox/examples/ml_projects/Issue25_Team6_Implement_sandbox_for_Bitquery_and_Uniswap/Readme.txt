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
source setenv.sh
docker-compose up -d
./init_airflow_setup.sh


####################################################
## Running project
####################################################
cd sorrentum_sandbox\examples\ml_projects\Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap/

./docker_bash.sh

docker> cd /cmamp/sorrentum_sandbox/examples/ml_projects/Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap/
docker> python3 download_to_csv.py --start_timestamp '2023-04-20T16:38:00' --target_dir 'uniswap_data'
docker> python3 download_to_db.py --start_timestamp '2023-04-20T16:38:00' --target_table 'uniswap_table'

####################################################
## Running project in Windows
####################################################

# Build contiainer using:
go to 
cd ~\sorrentum_sandbox\devops
wsl
source setenv.sh
docker-compose up -d
dos2unix init_airflow_setup.sh
./init_airflow_setup.sh


For windows users from project directory run:
wsl
dos2unix docker_bash.sh
./docker_bash.sh

docker> cd /cmamp/sorrentum_sandbox/examples/ml_projects/Issue25_Team6_Implement_sandbox_for_Bitquery_and_Uniswap/
docker> python3 download_to_csv.py --start_timestamp '2023-04-20T16:38:00' --target_dir 'uniswap_data'
docker> python3 download_to_db.py --start_timestamp '2023-04-20T16:38:00' --target_table 'uniswap_table'


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

