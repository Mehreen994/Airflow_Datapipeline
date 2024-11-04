from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from datetime import datetime
import json
import os 
#step 1) importing fetch_yotube_episodes function from fetch_Youtube_videos

from fetch_youtube_videos import fetch_youtube_episodes

#step2) defining the default arguments
default_args = {

    "owner" : "airflow",
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
#step3) intializing the dag
dag = DAG(
    dag_id = "Youtube_episode_fetch",
    default_args = default_args ,
    description = "Fetching episodes daily from youtube channel" ,
    schedule = "@daily",
    catchup = False,
 ) 

#step4)defining the task for fetching and storing the data:

def fetch_and_store(**context):
  

    # Fetch the YouTube episodes
   episodes = fetch_youtube_episodes(channel_id="UC_x5XG1OV2P6uZZ5FSM9Ttw", max_results=5)

    # Write to the JSON file
   json_file_path = os.path.join('/usr/local/airflow/dags', 'episodes.json')  # Use os.path.join for better path handling
   with open(json_file_path, 'w') as file:  # Ensure the file can be written
        json.dump(episodes, file, indent=4)  # Added indent for pretty printing
        print("Episodes successfully saved")

#defining python operator
perform_fetch_task = PythonOperator(
     task_id = "fetch_youtube_episodes",
     python_callable=fetch_and_store,
     dag = dag,
    
   )

perform_fetch_task

  