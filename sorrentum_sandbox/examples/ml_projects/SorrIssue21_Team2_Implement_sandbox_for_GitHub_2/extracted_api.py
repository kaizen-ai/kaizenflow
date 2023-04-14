#!/usr/bin/env python3
##Import requried packages
import pandas as pd
import psycopg2 as psycop



#Connection String for main DB
def get_db_connection(query_var) :       
    connection = psycop.connect(
        host="host.docker.internal",                                      
        dbname="airflow",
        port=5532,
        user="postgres",
        password="postgres")
    drt_cursor=connection.cursor()
    drt_cursor.execute(query_var)
    data=drt_cursor.fetchall()
    connection.close()
    return pd.DataFrame(data)

#Pulling Data from Issues Table-
issues_check_query= "SELECT * FROM github_issues"
issues_df = get_db_connection(issues_check_query)
issues_df = pd.DataFrame(issues_df,columns=['id','number','title','created_at','updated_at','closed_at','author_association','comments','body','user_login','user_id','Crypto_Name',
'Extension']) 
print("Pulling The Issues df:",issues_df.head(2))



#Pulling Data from Commits Table-
commits_check_query= "SELECT * FROM github_commits"
commits_df = get_db_connection(commits_check_query)
commits_df = pd.DataFrame(commits_df, columns = ['total', 'week', 'days', 'Crypto_Name', 'Extension', 'Sun', 'Mon',
       'Tue', 'Wed', 'Thur', 'Fri', 'Sat'])

print("Pulling the Commits df:",commits_df.head(2))


#Pulling Data from Main Table-
main_check_query= "SELECT * FROM github_main"
df = get_db_connection(main_check_query)
main_df=pd.DataFrame(df)
print("Pulling the Main df:",main_df.head(2))


#Inserting to separte csv..
print("Inserting to CSV Files")

issues_df.to_csv("github_issues.csv",encoding='utf-8', index=False)
commits_df.to_csv("github_commits.csv",encoding='utf-8', index=False)
main_df.to_csv("github_main.csv",encoding='utf-8', index=False)
print("Insertion Done!")

