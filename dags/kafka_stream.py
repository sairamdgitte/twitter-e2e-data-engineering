from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import uuid
import pandas as pd

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 8, 1, 10, 00)

}

def get_data():
    import pandas as pd
    from json import loads, dumps
    import os, logging

    df = pd.read_csv('./dataframe.csv')
    
    logging.info(f"DataFrame shape: {df.shape}")
    df.drop(['Unnamed: 0', 'mentions'], axis=1, inplace=True)
    
    return df
    

def format_data(data):  
    import os, logging  
    result = data.to_json(orient='records')
    # logging.info(f"Formatted data to JSON: {result[:100]}...")
    return result
    

def stream_data():
    import json
    from json import loads, dumps
    from kafka import KafkaProducer
    import time
    import logging
    import traceback

    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    streaming = True
    while streaming:
        if time.time() > curr_time + 60: #1minute
            break
        try:
            
            df = get_data()
            data_json = format_data(df)
            data_list = json.loads(data_json)
            
            for record in data_list:
                
                record['id'] = str(uuid.uuid4())
                record['lat'] = str(record['lat'])
                record['lng'] = str(record['lng'])
                record['sentiment'] = str(record['sentiment'])
                # logging.info(f"Sending record: {record}")
                producer.send('users_created', json.dumps(record).encode('utf-8'))
            
            streaming = False

            logging.info(f"Sent {len(data_list)} records")
            producer.flush()
        except Exception as e:
            logging.error(f'An error occured: {e}')
            logging.error(traceback.format_exc())
            continue
 
# stream_data()
with DAG('user_automation', default_args=default_args, schedule='@daily', catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id = 'stream_twitter_data_from_api',
        python_callable=stream_data
    )

