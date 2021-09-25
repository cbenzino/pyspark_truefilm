# pyspark_truefilm
Truefilm
Installation Steps:
1) The First Step is to install apache spark on a local machine or set up a spark EMR cluster.
    reference for local installation: https://www.datacamp.com/community/tutorials/installation-of-pyspark
2) Download spark-XML jar file and Postgres driver jar files.
    reference for spark-xml jars: https://jar-download.com/?search_box=spark-xml (download spark-xml.jar file)
    reference for postgres jdbc jar: https://jdbc.postgresql.org/download.html (download PostgreSQL JDBC 4.2 Driver, 42.2.23 file)
3) Extract these jar files from above downloaded zip files and place in following path in spark directory
    reference path: Users/benjaminnelson/spark-3.0.0-bin-hadoop2.7/jars

Execution Steps:
1) Extract the zip files and place both unzipped XML and CSV input files in the same directory
   where code file (movie_analytics) exists.
2) Navigate to your spark installation folder Users/benjaminnelson/spark-3.0.0-bin-hadoop2.7/ in cmd
3) execute pyspark script using below command in cmd
    bin/spark-submit <path_to_script>/movie_analytics.py
3) This script utilizes properties.json file to identify deployment mode, spark app name, CSV and XML file paths and configuration for PostgreSQL database connectivity and do the ETL process i.e load data from files into a dataframe, transform that data, and load it into Postgres with only 1000 records with highest revenue ratio.
4) Once the script is executed, it will load the data into the Postgres table and you can query any table as below
    SELECT * FROM <schema.table>;
    e.g SELECT * FROM practice.movies_data 
