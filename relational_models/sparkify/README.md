
# Sparkify database (DE course project)

## Scenario

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Data Model

Data has been organized in a star schema with the 'songplays' table as fact table, and the 'songs', 'artists', 'users', and 'time' tables as dimension tables

### Data Model Justification

Given the requirement for optimization for a specific class of queries, some level of denormalization is expected. Queries are simple, the data can be modelled efficiently for the purpose using only one-to-one relationships, and there is no need for normalized dimension tables. It follows that a star schema is a sufficient choice.

### Data Quality Constraints:
1. All fields not relating to personal information or location are considered mandatory.  
2. All records are complete as per 1. Records violating data quality constraints will be dropped.  
3. All records are unique as per 1. Duplicate records will be ignored.  
4. All records from which multiple tables are populated are treated atomically to maintain alignment between those tables.

## Directory Contents

data/: contains source data for tables  
> log\_data: user activity logs; directory partitioned by date.  
> song\_data: song metadata; directory partitioned by first three letters of song track ID.  

**create\_tables.py**: drops conflicting tables if they exist and creates the required tables.  
**etl.py**: loads, parses and inserts data from the data/ directory into the relevant tables.  
**setup.sh**: installs the required python libraries. Pulls and runs the latest postgres docker image.  
**sql_queries.py**: contains all SQL queries to be run in **create_tables.py** and **etl.py**  

## Quickstart

1. Run **setup.sh** to install the python dependencies, fetch the postgres docker image, and start the container.  
2. Run **create_tables.py** to remove any tables that may conflict and create the required tables. This can be used to reset the DB.  
3. Run **etl.py** to load, parse, and insert data into the following tables:  
    - songplays  
	- songs  
	- artists  
	- users  
	- time  

