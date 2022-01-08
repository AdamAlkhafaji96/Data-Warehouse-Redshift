# Create a Data Warehouse on AWS for Sparkify
Sparkify (fictional music streaming app) wants to move their processes and data onto the cloud in the interest of supporting their expanded user and song databases. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Datasets
    Song data: s3://udacity-dend/song_data
    Log data: s3://udacity-dend/log_data
    
Song dataset is a subset of the Million Song Dataset.
Log datasets have been created by this event simulator.
    
# ELT Process (see DFD.png)
1. Extract the data from S3
2. Load the data into staging tables on Redshift
3. Transform the data into a set of dimensional tables
4. Run queries for testing given by the Sparkify analytics team

# Database Schema Design (see ERD.png)
Fact Table: songplays
Dimension Tables: users, songs, artists, time

# Project Structure
    README.MD 
    DFD.png - Data Flow Diagram
    ERD.png - Entity Relationship Diagram 
    dwh.cfg - contains cluster configuration information 
    sql_queries.py - contains sql queries for dropping and creating tables as well as copying and inserting data.
    create_tables.py - used to drop and create tables in redshift.
    etl.py - copy data from S3 to staging tables in Redshift, then transform the data into a dimensonal model.   
    
# Instructions 
1st. Create cluster 
    Recommended: In the same region as the S3 buckets ('us-west-2')
2nd. Populate dwh.cfg file with [CLUSTER] HOST, DB_NAME, DB_USER, DB_ and [IAM_ROLE] information 
3rd. Open terminal and run create_tables.py
4th. Upon table creation run etl.py 
5th. Conduct analysis on data