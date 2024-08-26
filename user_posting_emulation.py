import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml
from datetime import datetime

# Load database credentials and API Invoke URL from db_creds.yml
with open('db_creds.yml', 'r') as file:
    db_config = yaml.safe_load(file)

random.seed(100)

class AWSDBConnector:

    def __init__(self, db_config):
        self.HOST = db_config['HOST']
        self.USER = db_config['USER']
        self.PASSWORD = db_config['PASSWORD']
        self.DATABASE = db_config['DATABASE']
        self.PORT = db_config['PORT']
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

def datetime_converter(o):
    if isinstance(o, datetime):
        return o.isoformat()
    
# Load database credentials from db_creds.yaml
with open('db_creds.yml', 'r') as file:
    db_config = yaml.safe_load(file)

new_connector = AWSDBConnector(db_config)
API_ENDPOINT = db_config['API_INVOKE_URL']

def send_to_kafka(topic, data):
    invoke_url = f"{API_ENDPOINT}/topics/{topic}"
    
    # Convert datetime objects in data to strings
    payload = json.dumps({
        "records": [
            {
                "value": data
            }
        ]
    }, default=datetime_converter)

    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    try:
        response = requests.request("POST", invoke_url, headers=headers, data=payload)
        response.raise_for_status()
        print(f"Successfully sent data to {topic}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to send data to {topic}: {str(e)}")
        if response:
            print(f"Response Status Code: {response.status_code}")
            print(f"Response Text: {response.text}")
        
def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            #Send to Kafka
            send_to_kafka("12d03c8b5ccd.pin", pin_result)
            send_to_kafka("12d03c8b5ccd.geo", geo_result)
            send_to_kafka("12d03c8b5ccd.user", user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
