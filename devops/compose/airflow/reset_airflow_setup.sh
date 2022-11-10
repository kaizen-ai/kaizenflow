 # Execute
 docker exec \
     --env AIRFLOW__CORE__LOAD_EXAMPLES=False \
     -ti airflow_cont \
     airflow db reset && \
     echo "Initialized airflow DB"
