CREATE DATABASE im_db_dev;
CREATE DATABASE im_db_pre_prod;
CREATE DATABASE im_db_prod;
CREATE USER im_dev WITH PASSWORD '***REMOVED***';
CREATE USER im_pre_prod WITH PASSWORD 'hjadhnuh2193nbodwaWa';
CREATE USER im_prod WITH PASSWORD 'hjadhnuh2193nbodwaWa';
GRANT ALL PRIVILEGES ON DATABASE im_dev TO im_db_dev;
GRANT ALL PRIVILEGES ON DATABASE im_pre_prod TO im_db_pre_prod;
GRANT ALL PRIVILEGES ON DATABASE im_prod TO im_db_prod;