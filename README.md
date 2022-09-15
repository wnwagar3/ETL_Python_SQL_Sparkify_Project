# Introduction

- The goal of this project is to create an ETL pipeline for a startup called 'Sparkify' so the analytics team can analyze date on songs and user activity. The company would like a data engineer to create a database, specifically using Postgres, with tables designed to optimize queries for various searches. Currently, there is no standardized, efficient way to query their data. 

# Data

- The data will come from a subset of the 'Million Song Dataset.'

# Schema

- The data we will be using and crafting the ETL pipeline for is organized in a star schema arrangement. The songsplay table being the center of the start with the users, time, songs, and artists tables making points on the start connecting to the songsplay table. The songsplay table contains a foreign key that is a primary key for each other table respectively. 

# Steps to Complete Project

- The source code for this project is available in 3 files, python scripts to be specific. 

- sql_queries.py has all the queries needed to create and drop tables from our dataset as well as having an SQL query to get song_id and artist_id from other tables.

- create_tables.py creates the database and establishes a user connection as well as creating or dropping (if exists) all the table required to create our pipeline and this file references the sql_queries.py script mentioned above.

- etl.py contains a script to process files and load them into tables

- We will follow the instructions in the etl.ipynb Jupyter Notebook to develop an ETL process for each table. When we have completed the etl.ipynb notebook we will then run test.ipynb to confirm that our work was successful and that records are, in fact, inserted into each table. 

- It is important to note that it is necessary to rerun create_table.py to reset our tables each time we run code in the etl.ipynb notebook when we make changes or correct errors.

- Once the etl.ipynb notebook has been completed, we will use it to complet etl.py which will process the entire dataset. 

- At the conclusion of successful implementation of our ETL pipeline we will create README.md file that explains the purpose and context of this project, the steps to complete the project, and explain the schema used in this project.