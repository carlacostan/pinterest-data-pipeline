import random
from time import sleep
import requests
import json
from sqlalchemy import text
from db_connector import AWSDBConnector  

# Initialize the database connector
new_connector = AWSDBConnector()

# API Invoke URLs
base_invoke_url = "https://8dpb920pic.execute-api.us-east-1.amazonaws.com/streams"
pin_invoke_url = f"{base_invoke_url}/streaming-12d03c8b5ccd-pin/record"
geo_invoke_url = f"{base_invoke_url}/streaming-12d03c8b5ccd-geo/record"
user_invoke_url = f"{base_invoke_url}/streaming-12d03c8b5ccd-user/record"

def check_api_endpoint(invoke_url):
    """
    Check if the API endpoint is accessible.
    """
    try:
        response = requests.get(invoke_url, timeout=5)
        if response.status_code == 200:
            print(f"API endpoint {invoke_url} is accessible.")
        else:
            print(f"API endpoint {invoke_url} returned status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to connect to API endpoint {invoke_url}: {e}")

def send_to_kinesis(invoke_url, data):
    """
    Sends the data to the specified Kinesis stream via API.
    """
    payload = json.dumps({
        "StreamName": invoke_url.split('/')[-2],  # Extracts the stream name from the URL
        "Data": data,
        "PartitionKey": str(random.randint(1, 1000))
    }, default=str)  

    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.put(invoke_url, headers=headers, data=payload, timeout=10)
        print(f"Sending data to {invoke_url} with payload: {payload}")
        
        if response.status_code == 200:
            print(f"Data successfully sent to {invoke_url.split('/')[-2]}")
        elif response.status_code == 403:
            print(f"Forbidden: Check IAM permissions for the Kinesis stream {invoke_url.split('/')[-2]}")
        elif response.status_code == 500:
            print(f"Internal Server Error: Something went wrong on the server while sending to {invoke_url.split('/')[-2]}")
        else:
            print(f"Failed to send data to {invoke_url.split('/')[-2]}: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request to {invoke_url} failed: {e}")

def run_infinite_post_data_loop():
    """
    Runs an infinite loop to continuously retrieve and send data to Kinesis streams.
    """
    print("Starting infinite data post loop...")
    check_api_endpoint(pin_invoke_url)
    check_api_endpoint(geo_invoke_url)
    check_api_endpoint(user_invoke_url)

    while True:
        sleep(random.randrange(1, 3))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            # Pinterest data
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                send_to_kinesis(pin_invoke_url, pin_result)

            # Geolocation data
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                send_to_kinesis(geo_invoke_url, geo_result)

            # User data
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = dict(row._mapping)
                send_to_kinesis(user_invoke_url, user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()
