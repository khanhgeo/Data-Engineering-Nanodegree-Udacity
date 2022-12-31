This repo contains data engineering projects that I completed to fulfill the Data Engineering Nanodegree program requirement. Following are short descriptions of these five projects. 


Project 1 - Data Modeling

In this project, I modeled user activity data for a music streaming app called Sparkify. The project is done in two parts. I created a database, imported data stored in CSV and JSON files, and modeled the data. Firstly, I did with a relational model in Postgres, and then with a NoSQL data model with Apache Cassandra. I designed the data models to optimize queries for understanding what songs users are listening to. For PostgreSQL, I also defined Fact and Dimension tables and inserted data into these new tables. For Apache Cassandra, I modeled data to help the data team at Sparkify answer queries about app usage.


Project 2 - Cloud Data Warehousing

I built an ELT pipeline in this project that extracts Sparkify’s data from S3, Amazon’s popular storage system. From there, data was staged in Amazon Redshift and transformed into a set of fact and dimensional tables for the Sparkify analytics team to continue finding insights into what songs their users are listening to.


Project 3 - Data Lakes with Apache Spark

In this project, I built an ETL pipeline for a data lake. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in the app. I loaded data from S3, processed the data into analytics tables using Spark, and loaded them back into S3. In this project, I deployed this Spark process on a cluster using AWS.


Project 4 - Data Pipelines with Apache Airflow

In this project, I continued my work on Sparkify’s data infrastructure by creating and automating a set of data pipelines. I used the up-and-coming tool Apache Airflow, developed and open-sourced by Airbnb and the Apache Foundation. I configured and scheduled data pipelines with Airflow, setting dependencies, triggers, and quality checks.


Project 5 - Data Engineering Capstone

Design and build a single source-of-truth data warehouse for data analysis purpose. The data warehouse will consist of fact and dimensional tables in which data are ingested and transformed from I94 immigration data, world temperature data, US cities demographic and airport code table 

* [I94 Immigration Data](https://travel.trade.gov/research/reports/i94/historical/2016.html): This data comes from the US National Tourism and Trade Office in SAS format, which contains statistical information of international visitor arrival  by select countries and regions. 
* [World Temperature Data](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data): This dataset came from Kaggle in CSV format,storing monthly average temperature data at different countries worldwide.
* [U.S. City Demographic Data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/): This data comes from OpenSoft in CSV format containing information about all US cities demographics.
* [Airport Code Table](https://datahub.io/core/airport-codes#data): This is a simple table of airport codes and corresponding cities.