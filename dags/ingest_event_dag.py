import os
import json
import logging
import traceback
from datetime import datetime
from typing import List, Dict

import requests
from dotenv import load_dotenv
from fetchfox_sdk import FetchFox

from airflow import DAG
from airflow.operators.python import PythonOperator

# Load environment variables
load_dotenv()

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# API clients
fox = FetchFox(api_key=os.getenv("FETCHFOX_API_KEY"))
eventbrite_api_key = os.getenv("EVENTBRITE_API_KEY")

def retrieve_event_ids(category: str, location_id: str, page_no: int, date: str, **context) -> List[int]:
    url = f"https://www.eventbrite.com/d/{location_id}/{category}--events/?page={page_no}&start_date={date}&end_date={date}"
    items = fox.extract(url, {"eventbite_event_id": "event_id"})
    event_ids = []
    for item in items:
        try:
            event_ids.append(int(item["eventbite_event_id"]))
        except Exception:
            logger.error(f"Invalid event ID: {item}")
    logger.info(f"Retrieved {len(event_ids)} event IDs for {location_id}, {category}, {date}, page {page_no}")
    return event_ids

def fetch_event_details(event_id: int) -> Dict:
    url = f"https://www.eventbriteapi.com/v3/events/{event_id}"
    headers = {"Authorization": f"Bearer {eventbrite_api_key}"}
    response = requests.get(url, headers=headers)
    if response.ok:
        return response.json()
    else:
        logger.error(f"Failed to fetch event {event_id}: {response.status_code}")
        return None

def fetch_details_for_event_ids(category: str, location_id: str, date: str, page_no: int, **context) -> List[Dict]:
    task_id = f'retrieve_event_ids_{location_id}_{category}_{date}_{page_no}'
    event_ids = context['ti'].xcom_pull(task_ids=task_id, key='return_value')
    if not event_ids:
        logger.warning(f"No event IDs found for {location_id}, {category}, {date}, page {page_no}")
        return []

    event_details = []
    for event_id in event_ids:
        try:
            details = fetch_event_details(event_id)
            if details:
                event_details.append({
                    "event_id": details.get("id"),
                    "event_name": details.get("name", {}).get("text"),
                    "event_url": details.get("url"),
                    "event_start_time": details.get("start", {}).get("local"),
                    "event_end_time": details.get("end", {}).get("local"),
                    "event_description": details.get("description", {}).get("text"),
                    "venue_id": details.get("venue_id"),
                    "category_id": details.get("category_id"),
                    "is_free": details.get("is_free"),
                })
        except Exception as e:
            logger.error(f"Error fetching details for event {event_id}: {e}")
            logger.error(traceback.format_exc())
    return event_details

def write_all_event_details(**context):
    ti = context['ti']
    all_event_details = []

    for task_id in context['task'].upstream_task_ids:
        details = ti.xcom_pull(task_ids=task_id, key='return_value')
        if details:
            all_event_details.extend(details)

    with open("event_details.json", "w") as f:
        json.dump(all_event_details, f, indent=2)

    logger.info(f"Wrote {len(all_event_details)} events to event_details.json")
    return all_event_details

def generate_search_terms():
    return {
        'location_ids': ['pa--philadelphia', 'ma--boston'],
        'categories': ['business', 'technology'],
        'page_nos': list(range(2)),
        'dates': ['2025-05-09', '2025-05-10']
    }

with DAG(
    dag_id="ingest_event_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: logger.info("Starting DAG")
    )

    search_terms = generate_search_terms()
    event_detail_tasks = []

    for location_id in search_terms['location_ids']:
        location_task = PythonOperator(
            task_id=f"location_{location_id}",
            python_callable=lambda location_id=location_id: logger.info(f"Processing {location_id}"),
        )
        start >> location_task

        for category in search_terms['categories']:
            for date in search_terms['dates']:
                for page_no in search_terms['page_nos']:
                    op_kwargs = {
                        'category': category,
                        'location_id': location_id,
                        'date': date,
                        'page_no': page_no,
                    }

                    retrieve_task = PythonOperator(
                        task_id=f"retrieve_event_ids_{location_id}_{category}_{date}_{page_no}",
                        python_callable=retrieve_event_ids,
                        op_kwargs=op_kwargs
                    )

                    fetch_task = PythonOperator(
                        task_id=f"fetch_event_details_{location_id}_{category}_{date}_{page_no}",
                        python_callable=fetch_details_for_event_ids,
                        op_kwargs=op_kwargs
                    )

                    location_task >> retrieve_task >> fetch_task
                    event_detail_tasks.append(fetch_task)

    write_task = PythonOperator(
        task_id="write_all_event_details",
        python_callable=write_all_event_details
    )

    for task in event_detail_tasks:
        task >> write_task
