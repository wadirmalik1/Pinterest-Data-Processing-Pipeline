import os
from pyspark.sql import SparkSession
from tkinter.tix import COLUMN
import pyspark.sql.functions as f
import pyspark.sql.types as T

# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.postgresql:postgresql:42.2.10 pyspark-shell'
# specify the topic we want to stream data from.
kafka_topic_name = "PintrestTopic2"
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
        .builder \
        .appName("KafkaStreaming ") \
        .getOrCreate()

# Only display Error messages in the console.
spark.sparkContext.setLogLevel("WARN")

# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

# Select the value part of the kafka message and cast it to a string.
stream_df = stream_df.selectExpr("CAST(value as STRING)")

schema = T.ArrayType(T.StructType([
                    T.StructField("category", T.StringType(), True),
                    T.StructField("index", T.StringType(), True),
                    T.StructField("unique_id", T.StringType(), True),
                    T.StructField("title", T.StringType(), True),
                    T.StructField("description", T.StringType(), True),
                    T.StructField("follower_count", T.StringType(), True),
                    T.StructField("tag_list", T.StringType(), True),
                    T.StructField("is_image_or_video", T.StringType(), True),
                    T.StructField("image_src", T.StringType(), True),
                    T.StructField("downloaded", T.StringType(), True),
                    T.StructField("save_location", T.StringType(), True)]))

parsed = stream_df.withColumn("temp", f.explode(f.from_json("value", schema))).select("temp.*")


# Clean the data
parsed = parsed.withColumn("title", f.regexp_replace("title", "No Title Data Available", "unknown")) \
    .withColumn("description", f.regexp_replace("description", "No description available Story format", "unknown")) \
    .withColumn("follower_count", f.regexp_replace("follower_count", "User Info Error", "unknown")) \
    .withColumn("tag_list", f.regexp_replace("tag_list", "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", "unknown"))

parsed.printSchema()
# Upload data onto postgres
def save_batch(df, epoch_id):
    df.write.mode("append") \
        .format("jdbc")\
        .option("url", f'jdbc:postgresql://localhost:5432/postgres') \
        .option("driver", "org.postgresql.Driver").option("dbtable", "experimental_data") \
        .option("user", "postgres").option("password", "afgangalo12").save()


# outputting the messages to the console 
parsed.writeStream \
    .foreachBatch(save_batch) \
    .start() \
    .awaitTermination()



#.option("checkpointLocation","/Users/wadirmalik/Desktop/kafka_2.12-3.2.1/checkpoint") \
       #.format("console") \
    #.outputMode("update") \

 
#.withColumn("position" , f.col("position").cast(T.IntegerType())) \
    #.withColumn("downloaded" , f.col("downloaded").cast(T.IntegerType())) \
    #.withColumn("unique_id" , f.col("unique_id").cast(T.StringType())) \


