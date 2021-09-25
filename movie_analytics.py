import json
#import helper functions and Types from Spark
from pyspark.sql.functions import col, date_format, desc, asc, broadcast, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType
#import SparkContext and SparkConf
from pyspark import SparkContext, SparkConf, SQLContext
import os

script_dir = os.path.dirname(__file__)
file_path = os.path.join(script_dir, 'properties.json')

with open(file_path) as json_file:
    data = json.load(json_file)

#setup configuration property, set the master URL and set an application name
conf = SparkConf().setMaster(data.get("deployment_mode")).setAppName(data.get('app_name'))
#start spark cluster if already started then get it else start it
sc = SparkContext.getOrCreate(conf=conf)
#initialize SQLContext from spark cluster
sqlContext = SQLContext(sc)

#Filepath variable for your file location directory
csv_path = os.path.join(script_dir, data.get("csv_file_path"))

#dataframe set header property true for the actual header columns
movies_df=sqlContext.read.options(header=True, delimiter=',', escape='"').csv(csv_path)\
    .selectExpr("title as Title", "budget as Budget", "revenue as Revenue", "release_date",
                "vote_average as Rating", "production_companies as Production_Companies")\
    .withColumn("Year", date_format(col("release_date"), "y")).withColumn("Profit", col("Revenue")-col("Budget"))\
    .withColumn("Ratio", (col("Profit")/col("Budget"))*100)\
    .drop("release_date", "Profit").na.fill(value=0,subset=["Ratio"])

# movies_df.cache()
# print(movies_df.count()) #45572

xml_schema = StructType([
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("abstract", StringType(), True)
])

xml_path = os.path.join(script_dir, data.get("xml_file_path"))
xml_df = sqlContext.read \
    .format("com.databricks.spark.xml") \
    .option("rootTag", "feed") \
    .option("rowTag", "doc") \
    .load(xml_path, schema=xml_schema).selectExpr("title as Movie_Title", "url", "abstract")\
    .withColumn('Movie_Title', regexp_replace('Movie_Title', 'Wikipedia: ', '')).repartition("Movie_Title")

# xml_df.cache()
# print(xml_df.count()) # 6369275

joined_df = xml_df.join(broadcast(movies_df), xml_df.Movie_Title == movies_df.Title)

final_df = joined_df.drop("Movie_Title").sort(desc("Ratio")).limit(1000)
final_df.cache()
final_df.show()

mode = data.get("jdbc_mode")
url = data.get("jdbc_url")
properties = {"user": data.get("jdbc_user"), "password": data.get("jdbc_password"), "driver": data.get("jdbc_driver")}
final_df.write.jdbc(url=url, table=data.get("jdbc_table"), mode=mode, properties=properties)

print("Rows loaded into Postgresql database: ", final_df.count())





