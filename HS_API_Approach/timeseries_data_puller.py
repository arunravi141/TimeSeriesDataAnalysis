import logging
import requests
import pandas as pd
import os

# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Please setup these initial variables
ACCESS_TOKEN = '8d7912fd3c6b40c0bdcabe88e1fadc35'
USER_FLOWS = ["VideoCall_WhatsApp_Android_Caller", "VideoCall_Facetime_iOS_Callee"]
NUM_OF_SESSIONS_TO_BE_FETCH = 1
# define the time-series-key
TIME_SERIES_CATEGORY = "video_quality_mos"


BASE_URL = "https://meta-api.headspin.io/v0/sessions"
HEADERS = {
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }

def fetch_sessions(user_flow_name):
    request_url = BASE_URL
    tag_value = f"user_flow:{user_flow_name}"
    params = {"include_all": "true", "num_sessions": NUM_OF_SESSIONS_TO_BE_FETCH, "tag": tag_value}
    response = requests.get(request_url, params=params, headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        # print(data)
        session_ids = [session['session_id'] for session in data['sessions'] if session['status'] == 'passed']
        return session_ids
    else:
        print(f"Request failed with status code: {response.status_code}")
        return None

def fetch_label_details(session_id):
    request_url = f"{BASE_URL}/{session_id}/label/list"
    params = {"category": TIME_SERIES_CATEGORY}
    response = requests.get(request_url, headers=HEADERS, params=params)
    
    if response.status_code == 200:
        data = response.json()
        # Assuming there is at least one label in the response
        if 'labels' in data and len(data['labels']) > 0:
            label = data['labels'][0]  # Get the first label or iterate if multiple labels
            start_time = label.get('start_time')
            ts_start = label.get('ts_start')
            end_time = label.get('end_time')
            return start_time, end_time, ts_start
        else:
            print("No labels found in the response")
            return None, None, None
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
        return None, None, None

def download_csv_file(session_id):
    request_url = f"{BASE_URL}/timeseries/{session_id}/download?key={TIME_SERIES_CATEGORY}"
    response = requests.get(request_url, headers=HEADERS)
    if response.status_code == 200:
        csv_filename = f"{session_id}_temp.csv"
        with open(csv_filename, "wb") as file:
            file.write(response.content)
        logger.info(f"Time series data for {session_id} downloaded successfully.")
        return csv_filename
    else:
        logger.error(f"Failed to download data for {session_id}. Status code: {response.status_code}, Response: {response.text}")
        return None

def filter_and_save_csv_data(csv_filename, start_time, end_time, ts_start, session_id, user_flow_name):
    try:
        df = pd.read_csv(csv_filename)
        # Convert the 'Time' column from milliseconds to seconds and add ts_start to get absolute time
        df['Absolute_Time'] = ts_start + df['Time']/1000 
        # Filter the DataFrame based on the provided label start_time and end_time
        filtered_df = df[(df['Time'] >= start_time) & (df['Time'] <= end_time)]
        # Save to appropriate directory
        directory = os.path.join("processed_data", user_flow_name)
        if not os.path.exists(directory):
            os.makedirs(directory)

        filtered_csv_filename = os.path.join(directory, f"{session_id}_{TIME_SERIES_CATEGORY}.csv")
        filtered_df.to_csv(filtered_csv_filename, index=False)
        logger.info(f"Filtered and converted data saved to {filtered_csv_filename}.")
        return filtered_csv_filename
    except Exception as e:
        logger.error(f"Error processing CSV file {csv_filename}: {e}")
        return None
    finally:
        if os.path.exists(csv_filename):
            os.remove(csv_filename)
            # logger.info(f"Original CSV file {csv_filename} deleted.")

def is_session_already_processed(session_id, user_flow_name):
    directory = os.path.join("processed_data", user_flow_name)
    processed_csv_filename = os.path.join(directory, f"{session_id}_{TIME_SERIES_CATEGORY}.csv")
    return os.path.exists(processed_csv_filename)


if __name__ == "__main__":
    for user_flow_name in USER_FLOWS:
        session_ids = fetch_sessions(user_flow_name)
        for session_id in session_ids:
            logger.info(f"********* Processing data for Session {session_id} *********")
            if is_session_already_processed(session_id, user_flow_name):
                logger.info(f"Session {session_id} already processed. Skipping...")
                continue
            try:
                label_start_time, label_end_time , label_start_absolute= fetch_label_details(session_id="0da7ae88-6428-11ef-8105-86a25e1fcef1")
                logger.info(f"Label Start time: {label_start_time}, Label End time: {label_end_time}")
                session_start_absolute = label_start_absolute - label_start_time/1000
                csv_file = download_csv_file(session_id)
                if csv_file:
                    filter_and_save_csv_data(
                        csv_filename=csv_file,
                        start_time=label_start_time,
                        end_time=label_end_time,
                        ts_start=session_start_absolute,
                        session_id=session_id,
                        user_flow_name=user_flow_name,
                    )
            except Exception as e:
                print(e)
        print("")

    logger.info("Processing complete.")
