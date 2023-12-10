
from tkinter.tix import COLUMN
import findspark
import os

findspark.init(os.environ["SPARK_HOME"])

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
 
# Adding the packages required to get data from S3  
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 pyspark-shell"



# Creating our Spark configuration
conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]') \
    .set('spark.sql.files.ignoreCorruptFiles', True)

sc=SparkContext(conf=conf)

# Configure the setting to read from the S3 bucket
accessKeyId="AKIAT2SOZXRGWBBOGWOA"
secretAccessKey="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"
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

# Create our Spark session
spark=SparkSession(sc)

# Read from the S3 bucket
df = spark.read.json("s3a://pinterest-data-34b08b92-1082-4d4f-a6e7-924a8ae2a66e/*.json") 

df = df.withColumnRenamed("index", "position") \
    .withColumn("title", f.regexp_replace("title", "No Title Data Available", "unknown")) \
    .withColumn("description", f.regexp_replace("description", "No description available Story format", "unknown")) \
    .withColumn("follower_count", f.regexp_replace("follower_count", "User Info Error", "unknown")) \
    .withColumn("tag_list", f.regexp_replace("tag_list", "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", "unknown")) \
    .withColumn("follower_count", f.regexp_replace("follower_count", "k", "000")) \
    .withColumn("follower_count", f.col("follower_count").cast(IntegerType())) \
    .withColumn("downloaded" , f.col("downloaded").cast(IntegerType())) 


df.show()

df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="pintrest", keyspace="pintrest_data") \
    .mode ("append") \
    .save()
