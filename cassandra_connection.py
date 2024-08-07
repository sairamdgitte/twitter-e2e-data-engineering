from cassandra.cluster import Cluster
import logging

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
                    cleaned_tweet TEXT
                    );
                    """)
    logging.info("Table created successfully!")

def cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()

        create_keyspace(session)
        create_table(session)

        return session
    except Exception as e:
        logging.error(f"Couldn't create the cassandra session due to exception {e}")