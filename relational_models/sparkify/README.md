
# Sparkify database (DE course project)

## Scenario

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Data Model

Data has been organized in a star schema with the 'songplays' table as fact table, and the 'songs', 'artists', 'users', and 'time' tables as dimension tables

### Data Model Justification

Given the requirement for optimization for a specific class of queries, some level of denormalization is expected. Queries are simple, the data can be modelled efficiently for the purpose using only one-to-one relationships, and there is no need for normalized dimension tables. It follows that a star schema is a sufficient choice.

## Directory Contents

data/: contains the log\_data (user activity logs; directory partitioned by date) and song\_data	(song metadata; directory partitioned by first three letters of song track ID) directories.  
create\_tables.py: drops conflicting tables if they exist and creates the required tables.  
etl.py: loads, parses and inserts data from the data/ directory into the relevant tables.  
setup.sh: installs the required python libraries. Pulls and runs the latest postgres docker image.  
