# Movie- Data Pipeline

Problem Statement:

We get three files everyday, in a local file system. Movie.csv, Rating.csv, tags.csv. 
We need to load these files into dataframes, aggregate them and store them into a relational database.

Below would be the high level steps in the pipeline:
  1. Extract data from files in dataframes
  2. Merge dataframes 
  3. Perform Aggregation
  4. Establish a connection with Postgres/MySql database
  5. Insert data into tables
 
