import json
import pathlib
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

dag = DAG(
    dag_id="download_rocket_local_enhanced",
    description="Download rocket pictures of recently launched rockets with enhanced error handling and performance.",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': airflow.utils.dates.days_ago(2),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
)

# Task to download the launches.json file
download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /opt/airflow/data/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag,
)


def _get_pictures():
    import os
    import requests
    from urllib.parse import urlparse
    # Ensure the images directory exists
    images_dir = "/opt/airflow/data/images"
    pathlib.Path(images_dir).mkdir(parents=True, exist_ok=True)

    # Load data from launches.json
    with open("/opt/airflow/data/launches.json") as f:
        launches = json.load(f)

    image_urls = [launch["image"] for launch in launches["results"] if launch["image"]] * 20

    def is_image_available(image_url):
        try:
            response = requests.head(image_url, timeout=10)

            if response.status_code == 200:
                print(f"Image available at {image_url}")

            return response.status_code == 200 and 'image' in response.headers.get('Content-Type', '')
        except requests.RequestException:
            return False

    def download_image(image_url, target_file):
        try:
            response = requests.get(image_url, stream=True, timeout=30)
            response.raise_for_status()
            with open(target_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded {image_url} to {target_file}")
            return True
        except requests.RequestException as e:
            print(f"Failed to download {image_url}: {e}")
            return False

    for image_url in image_urls:
        if is_image_available(image_url):
            image_filename = os.path.basename(urlparse(image_url).path)
            target_file = os.path.join(images_dir, image_filename)
            download_image(image_url, target_file)
        else:
            print(f"Image not available at {image_url}")


get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag
)

# Task to notify the number of images downloaded
notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /opt/airflow/data/images/ | wc -l) images in /opt/airflow/data/images."',
    dag=dag,
)

# Define the task dependencies
download_launches >> get_pictures >> notify