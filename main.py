import requests
import os
import json
import datetime
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone

SENSOR_INDEX = os.environ.get("SENSOR_INDEX")
BASE_URL = os.environ.get("BASE_URL")
API_KEY = os.environ.get("API_KEY")




def get_data(url, headers, params):
  try:
    response = requests.get(url, headers=headers, params=params, timeout=15)
    response.raise_for_status()  # Raise an exception for HTTP errors
    data = response.json()
    # get current time after fetching raw data to compare for freshness check
    now = datetime.now(timezone.utc)
    #report info and return data and curent time
    logging.info(f"Successfully fetched data from {url}")
    return data, now
  except requests.exceptions.HTTPError as e:
    logging.error(f"HTTP Error fetching data from {url}: {e} - Response: {getattr(response, 'text', 'No response text available')}")
    return None, now
  except requests.exceptions.ConnectionError as e:
    logging.error(f"Connection Error fetching data from {url}: {e}")
    return None, now
  except requests.exceptions.Timeout as e:
    logging.error(f"Timeout Error fetching data from {url}: {e}")
    return None, now
  except json.JSONDecodeError as e:
    logging.error(f"JSON Decode Error for {url}: {e} - Raw Response: {getattr(response, 'text', 'No response text available')}")
    return None, now
  except requests.exceptions.RequestException as e:
    logging.error(f"An unexpected Request Error occurred for {url}: {e}")
    return None, now


## check if sensor is working by comparing the last seen timestamp with the curent time and a given delay range set to 10 minutes
def check_sensor_freshness(data, now):

  delay_range=600 #10 minutes in seconds

  if not data:
    logging.warning(f"NO DATA RECIEVED for Sensor_index{SENSOR_INDEX }")
    return False
  if not data["sensor"]['last_seen']:
    logging.warning(f"Sensor data missing 'last_seen' timestamp. Cannot determine freshness.")
    return False

  #convert last seen and now_unix into datetime timestamps for more robust comparison
  last_seen_dt_utc = datetime.fromtimestamp(data["sensor"]['last_seen'], tz=timezone.utc)
  #fetch_dt_utc = datetime.fromtimestamp(now, tz=timezone.utc)

  time_difference = now - last_seen_dt_utc

  if time_difference < timedelta(seconds=delay_range):
    logging.info(f"Freshness check passed. Last seen {time_difference.total_seconds()}s ago (> {delay_range/60} minute  threshold).")
    return True
  else:
    logging.warning(f"Data is STALE. Last seen {time_difference.total_seconds()}s ago (> {delay_range/60} minute  threshold).")
    return False

## check validity of Enviromental data
def clean_and_validate_sensor_data(data):
  #copy data for safety
  cleaned_data = data.copy()

  def validate_and_update_data(key, min_value, max_value):
    value=cleaned_data.get(key)
    if not (isinstance(value, (int, float))):
      logging.warning(f"-Sensor {cleaned_data['sensor_index']}- {key} is not numeric. Set {key} to None.")
      cleaned_data[key]=None
      return # skip range check:

    if not (min_value <= value <= max_value):
      logging.warning(f"-Sensor {cleaned_data['sensor_index']}- {key} value: {value} out of range. Set {key} to None.")
      cleaned_data[key]=None

    elif isinstance(value, float):
      cleaned_data[key]=round(value, 2)

  validate_and_update_data("temperature", 0, 120)
  validate_and_update_data("humidity", 0, 100)
  validate_and_update_data("pressure", 750, 1300)
  validate_and_update_data("pm2.5", 0, 500)
  validate_and_update_data("pm2.5_alt", 0, 500)
  validate_and_update_data("visual_range", 0, 500)

  return cleaned_data


#OLD VERSİON. CLEANED WİTH validate_and_update_data(key, min_value, max_value) function

#  if not (isinstance(data["sensor_temperature"], (int, float)) and 0 <= data["sensor_temperature"] <= 120):
#    print(f"-Sensor {data['sensor_sensor_index']}- Invalid temperature value {data['sensor_temperature']}. Seting Temperatur to None.")
#    data["sensor_temperature"]= None

#  if not (isinstance(data["sensor_humidity"], (int, float)) and 0 <= data["sensor_humidity"] <= 100):
#    print(f"-Sensor {data['sensor_sensor_index']}- Invalid humidity value {data['sensor_humidity']}. Seting humidity to None.")
#    data["sensor_humidity"]= None

#  if not (isinstance(data["sensor_pressure"], (int, float)) and 750 <= data["sensor_pressure"] <= 1300):
#    print(f"-Sensor {data['sensor_sensor_index']}- Invalid pressure value {data['sensor_pressure']}. Seting pressure to None.")
#    data["sensor_pressure"]= None

#  if not (isinstance(data["sensor_pm2.5"], (int, float)) and 0 <= data["sensor_pm2.5"] <= 500):
#    print(f"-Sensor {data['sensor_sensor_index']}- Invalid pressure value {data['sensor_pm2.5']}. Seting pressure to None.")
#    data["sensor_pm2.5"]= None

def process_purpleair_data():
  endpoint = f"/sensors/{SENSOR_INDEX}/"
  url = f"{BASE_URL}{endpoint}"

  headers = {
    "X-API-Key": API_KEY,
    "Accept": "application/json"
    }
  params = {
    #"fields": "last_seen,rssi,humidity,temperature,pressure,pm2.5"
    "fields": "last_seen,rssi,humidity,temperature,pressure,pm2.5,pm2.5_alt,visual_range, pm2.5_6hour"
    }

  data, now = get_data(url, headers, params)
  data_freshness = check_sensor_freshness(data, now)

  if data_freshness == True:
    logging.info("freschness chek passed, cleaning data")

    #unpack sensor and statts data
    sensor_data = data.get("sensor")
    if sensor_data is None:
      logging.warning(f"API response for sensor {SENSOR_INDEX} did not contain a 'sensor' payload. Cannot proceed.")
      return None

    stats_dict = sensor_data.get('stats')
    if stats_dict and isinstance(stats_dict, dict):
      pm25_6hour_value = stats_dict.get('pm2.5_6hour')
      sensor_data['pm2.5_6hour'] = pm25_6hour_value
    else:
      # Handle the case where 'stats' is missing or not a dictionary
      sensor_data['pm2.5_6hour'] = None
      logging.warning("Stats sub-dictionary not found or is not a dictionary. Setting pm2.5_6hour to None.")
    sensor_data.pop('stats', None)

    sensor_data= clean_and_validate_sensor_data(sensor_data)
    df = pd.DataFrame(sensor_data, index=[0])
    unix_to_timestamp = datetime.fromtimestamp(sensor_data["last_seen"], tz=timezone.utc)
    df["date"]=unix_to_timestamp.strftime('%Y-%m-%d')
    df["time"]=unix_to_timestamp.strftime('%H:%M:%S')
    df['ingestion_timestamp_iso_utc'] = now.isoformat()
    df['ingestion_timestamp_unix_utc'] = int(now.timestamp())
    logging.info(f"Successfully prepared DataFrame for sensor {sensor_data['sensor_index']}.")
    return df

  else:
    logging.warning("DATA NOT FRESH. NO Data prepareable")
  #print(f"-------------DEBUGGİNG---------------\n {data} \n {now_unix}")

def lambda_handler(event, context):
  logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
  logging.info("Lambda function triggered: Starting PurpleAir data ingestion.")

  try:
    # Call your core data processing function
    final_df = process_purpleair_data() # This now returns the DataFrame

    if final_df is None:
      logging.warning("No data DataFrame was generated. Skipping upload.")
      return {
                'statusCode': 200,
                'body': json.dumps('Data processing completed, but no DataFrame to upload.')
            }

    # --- NOW: Save the DataFrame to AWS S3 ---
    s3_bucket_name = os.environ.get("S3_BUCKET_NAME")
    if not s3_bucket_name:
      logging.error("S3_BUCKET_NAME environment variable not set.")
      return {
                'statusCode': 500,
                'body': json.dumps('Configuration error: S3 bucket name missing.')
            }
    # Define a unique file name for the data in S3
    # Use ingestion timestamp and sensor ID to ensure uniqueness
    ingestion_time_iso = final_df['ingestion_timestamp_iso_utc'].iloc[0] if 'ingestion_timestamp_iso_utc' in final_df.columns else datetime.now(timezone.utc).isoformat()
    sensor_id_for_file = final_df['globalID'].iloc[0] if 'globalID' in final_df.columns else f"PA_Sensor_{SENSOR_INDEX}"

    # S3 object key (path and filename)
    # Example: data/purpleair/2025/06/11/PA_Sensor_156275_2025-06-11T11-00-00Z.parquet
    s3_key = f"data/purpleair/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/{sensor_id_for_file}_{ingestion_time_iso.replace(':', '-')}.parquet" # or .csv
    
    # Save DataFrame to S3 (e.g., as Parquet for efficiency)
    # Ensure pyarrow or fastparquet is installed in your deployment package for Parquet support
    import io
    buffer = io.BytesIO()
    final_df.to_parquet(buffer, index=False)
    buffer.seek(0) # Rewind the buffer to the beginning

    import boto3 # AWS SDK for Python
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=s3_bucket_name, Key=s3_key, Body=buffer.getvalue())

    logging.info(f"Successfully uploaded data for sensor {SENSOR_INDEX} to s3://{s3_bucket_name}/{s3_key}")

    return {
            'statusCode': 200,
            'body': json.dumps('Data ingestion and upload to S3 successful!')
        }

  except Exception as e:
    logging.error(f"An error occurred during Lambda execution: {e}", exc_info=True)
    return {
            'statusCode': 500,
            'body': json.dumps(f'Error during data ingestion: {e}')
        }