# Project: Data Modeling with Apache Cassandra

The sparklify team wants to analyze user data, for which they've been collecting songs and user activity on their new music streaming app.  
The analysis team is interested in understanding what songs users are listening to. 

The project demonstrates a Cassandra NoSQL ETL pipeline using Python. Given 3 queries, namely:  
**1. Finding the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession  = 4**

**2. The artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182**
    
**3. Every user name (first and last) in the music app history who listened to the song 'All Hands Against His Own'** 

The project models data by creating tables in Apache Cassandra to run queries. The jupyter notebook creates a streamlined CSV file to model and insert data into Apache Cassandra tables.

Each CSV file in the **event_data** folder is read into a single large CSV file, 'event_datafile_new.csv'.

## Problem Approach

In Cassandra, a table should be modelled after a single query. Since there are 3 queries, 3 tables are created.
The partition key also has to be chosen in a way that it minimizes the number of lookups or reads between partitions, i.e  
data should be evenly distributed.

A few statistical measures are used pre-table creation to display the spread of the data, using the mode and value_counts() method  
of pandas.

**Query 1**: The parameters in filter clause (WHERE) are session_id and item_in_session, so the PRIMARY KEY has been set as a combination of session_id and item_in_session.  
The number of distinct values in item_in_session are lower than session_id, which might indicate that when organized by item_in_session,  
the data would be less spread out. For this reason, item_in_session was chosen as PARTITION KEY.

**Query 2**: The first column in the WHERE clause is user_id, and the second column is session_id.  
We use a PARTITION KEY that is a combination of user_id, session_id to uniquely identify rows in the table.  
We then require a clustering column, since the query requires it to be ordered by item_in_session.

**Query 3**: The primary filtering clause is those that have listened to the song 'All Hands Against His Own'.  
We therefore use song as the partition key, and add first name, last name and user_id to uniquely identify   
users who may have the same first and last name but be different (by user_id).


## Additions

The queries outputs in the Project notebooks can be validated against validate.ipynb that contains pandas code for the same queries.  
The file Project_1B_Project_Template_Optimized.ipynb contains some optimizations to speed up the ELT process including inserts within the same    
csv file block, read from dataframe, usage of the cql COPY FROM command, which is currently commented out since it requires separate CSV  
files for separate tables. And usage of pandas to refer to columns by key rather than list index.

## Running the program locally
1. Use python 3.6.3
2. Install via conda (preferred) :  conda install --file conda_requirements.txt OR pip install -r requirements.txt
