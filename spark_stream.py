import logging
from datetime import datetime
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType
import uuid
from cassandra_connection import cassandra_connection
from pyspark.sql.functions import udf


logging.basicConfig(level=logging.INFO)

def create_keyspace(session):
    session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS spark_streams
                    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
                    """)
    logging.info("Keyspace created successfully!")

def create_table(session):
   
    session.execute("""
                    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                    id UUID PRIMARY KEY,
                    original_tweet TEXT,
                    tweet TEXT,
                    lat TEXT,
                    lng TEXT,
                    place TEXT,
                    date TEXT,
                    sentiment TEXT,
                    cleaned_tweet TEXT,
                    
                    );
                    """)
    logging.info("Table created successfully!")
    


def insert_data(session, **kwargs):
    print("Inserting data...")
    user_id = kwargs.get('id')
    cleaned_tweet = kwargs.get('cleaned_tweet')
    original_tweet = kwargs.get('original_tweet')
    tweet = kwargs.get('tweet')
    lat = kwargs.get('lat')
    lng = kwargs.get('lng')
    place = kwargs.get('place')
    date = kwargs.get('date')
    sentiment = kwargs.get('sentiment')
    

    try:
        session.execute("""
        INSERT INTO spark_streams.created_users (user_id, original_tweet, tweet, lat, lng, place, date, sentiment, cleaned_tweet)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, original_tweet, tweet, lat, lng, place, date, sentiment, cleaned_tweet))
        logging.info(f"Data inserted for {id} {date}")
    except Exception as e:
        logging.error(f"Data could not be inserted due to {e}")

    

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                    'org.apache.commons:commons-pool2:2.11.1'                   
                    ) \
            .config('spark.driver.host', '127.0.0.1') \
            .config('spark.driver.bindAddress', '127.0.0.1') \
            .config('spark.executor.bindAddress', '127.0.0.1') \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return s_conn  
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

      

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        
        logging.info(f"kafka dataframe created successfully")
        
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")
    
    return spark_df
    

def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        logging.info("Cassandra connection created successfully!")
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("original_tweet", StringType(), True),
        StructField("tweet", StringType(), True),
        StructField("lat", StringType(), True),
        StructField("lng", StringType(), True),
        StructField("place", StringType(), True),
        StructField("date", StringType(), True),
        StructField("sentiment", StringType(), True),
        StructField("cleaned_tweet", StringType(), True),
        
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*") 


    print(sel)
    return sel
    


if __name__ == "__main__":

    # Set up detailed logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    spark_conn = create_spark_connection()
    cassandra_conn = cassandra_connection()
    if spark_conn and cassandra_conn:
        
        
        
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)


        logging.info("Streaming is being started...")
        
        streaming_query = selection_df.writeStream \
                            .format("org.apache.spark.sql.cassandra") \
                            .option('checkpointLocation', '/tmp/checkpoint') \
                            .option('keyspace', 'spark_streams') \
                            .option('table', 'created_users')\
                            .outputMode('append') \
                            .start()
        streaming_query.awaitTermination()                
        
                

