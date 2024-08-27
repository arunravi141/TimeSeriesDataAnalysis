import logging
import psycopg2
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
import os

# Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Please setup these initial variables
ACCESS_TOKEN = '8d7912fd3c6b40c0bdcabe88e1fadc35'
USER_FLOWS = ["VideoCall_WhatsApp_Android_Caller", "VideoCall_Facetime_iOS_Callee"]
# time offset will allow to pull sessions between current_time, current_time - TIME_OFFSET
TIME_OFFSET = 3
TIME_SERIES_CATEGORY = "video_quality_mos"

BASE_URL = "https://meta-api.headspin.io/v0/sessions"
HEADERS = {
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }


def create_database_connection():
    try:
        return psycopg2.connect(
            database="replica_p54f31",
            user="replica_p54f31",
            password="5D6T4gKkIB4N",
            host="meta-postgres.headspin.io",
            port="5432"
        )
    except psycopg2.Error as e:
        logger.error(f"Error establishing a database connection: {e}")


def fetch_data_from_database(connection, query):
    with connection.cursor() as cursor:
        try:
            cursor.execute(query)
            return cursor.fetchall()
        except psycopg2.Error as e:
            logger.error(f"Error fetching data from the database: {e}")


def get_sessions(start_time, end_time, flow_names):
    connection = create_database_connection()
    logger.info('DB Connection Established')
    try:
        select_query = f"""SELECT DISTINCT sm.session_id, uf.name, 
                                sl.name, sl.start_time as label_start, sl.end_time as label_end
                        FROM session_metadata as sm
                        INNER JOIN user_flow as uf ON sm.user_flow_id = uf.user_flow_id
                        INNER JOIN session_labels as sl ON sm.session_id = sl.session_id AND sl.category = '{TIME_SERIES_CATEGORY}'
                        WHERE sm.start_time AT TIME ZONE 'MST' BETWEEN '{end_time}' AND '{start_time}'
                        AND uf.name IN ({','.join(map(repr, flow_names))})
                        AND sl.name in ('VIDEO_ANALYSIS_REGION_CALLER','VIDEO_ANALYSIS_REGION_CALLEE')
                        AND sm.status IN ('Passed','Failed')
                        """
        try:
            results = fetch_data_from_database(connection, select_query)
            logger.info('Successfully retrieved data from DB')
            return results
        except Exception as fetch_error:
            logger.error(f'Error fetching data from DB: {fetch_error}')
            return None
    except Exception as connection_error:
        logger.error(f'DB Connection Error: {connection_error}')
        return None
    finally:
        connection.close()


def download_csv_file(session_id):
    url = f"{BASE_URL}/timeseries/{session_id}/download?key={TIME_SERIES_CATEGORY}"
    response = requests.get(url, headers=HEADERS)
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


def get_session_timestamps(session_id):
    """
    Fetches timestamps for a given session using the HeadSpin API.
    """
    url = f"{BASE_URL}/{session_id}/timestamps"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        json_response = response.json()
        # all(key in json_response for key in ["capture-complete", "capture-ended", "capture-started"])
        return json_response["capture-started"]
    else:
        print(f"Failed to fetch timestamps. Status code: {response.status_code}")
        print(f"Response: {response.text}")
        return None

def is_session_already_processed(session_id, user_flow_name):
    directory = os.path.join("processed_data", user_flow_name)
    processed_csv_filename = os.path.join(directory, f"{session_id}.csv")
    return os.path.exists(processed_csv_filename)


if __name__ == "__main__":
    logger.info(datetime.now())
    logger.info("Session Pulling From DB started")
    
    current_time = datetime.now(timezone.utc)
    start_time = current_time.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    end_time = (current_time - timedelta(hours = TIME_OFFSET)).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

    logger.info(f"Data Fetching Time Frame --> Start time: {start_time}, End time: {end_time}")
    if start_time and end_time and USER_FLOWS:
        session_ids = get_sessions(start_time, end_time, USER_FLOWS)
        if session_ids:
            for session in session_ids:
                session_id, user_flow_name, label_name, label_start_time, label_end_time = session

                logger.info(f"********* Processing data for Session {session_id} *********")
                if is_session_already_processed(session_id, user_flow_name):
                    logger.info(f"Session {session_id} already processed. Skipping...")
                    continue
                try:
                    csv_file = download_csv_file(session_id)
                    session_start_absolute = get_session_timestamps(session_id)
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
        else:
            logger.warning("No sessions found matching the criteria.")
    else:
        logger.warning('Missing required values: start_time, end_time, or USER_FLOWS.')

    logger.info("Processing complete.")
