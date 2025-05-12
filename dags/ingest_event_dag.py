
import os, re, json, logging, sys, traceback

from datetime import datetime
from typing import List, Dict
from dotenv import load_dotenv

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

from db.MetadataHandler import MetadataHandler

# Load environment variables
load_dotenv()

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

eventbrite_api_key = os.getenv("EVENTBRITE_API_KEY")

PAGE_NOS = 5

def retrieve_event_ids(event_type: str, state_code: str, city_code: str, page_no: int, date: str, **context) -> List[int]:
    url = f"https://www.eventbrite.com/d/{state_code}--{city_code}/{event_type}--events/?page={page_no}&start_date={date}&end_date={date}"
    logger.info(f"Retrieving event IDs from {url}")
    response = requests.get(url)
    if not response.ok:
        logger.error(f"Failed to retrieve event IDs from {url}: {response.status_code}")
        return []
    
    text = response.text
    
    pattern = r'"eventbrite_event_id"\s*:\s*"(\d+)"'
    event_ids = [int(event_id) for event_id in re.findall(pattern, text)]
    logger.info(f"Event IDs: {event_ids}")
    logger.info(f"Retrieved {len(event_ids)} event IDs for {city_code}, {state_code}, {event_type}, {date}, page {page_no}")
    return event_ids

def fetch_event_details(event_id: int) -> Dict:
    url = f"https://www.eventbriteapi.com/v3/events/{event_id}"
    headers = {"Authorization": f"Bearer {eventbrite_api_key}"}
    response = requests.get(url, headers=headers)
    if response.ok:
        logger.info(f"Fetched event {event_id}: {response.json()}")
        return response.json()
    logger.error(f"Failed to fetch event {event_id}: {response.status_code}")
    return None

def fetch_details_for_event_ids(event_type: str, state_code: str, city_code: str, date: str, page_no: int, **context) -> List[Dict]:
    task_id = f'retrieve_event_ids_{state_code}--{city_code}_{event_type}_{date}_{page_no}'
    event_ids = context['ti'].xcom_pull(task_ids=task_id, key='return_value')
    if not event_ids:
        logger.warning(f"No event IDs found for {state_code}, {city_code}, {event_type}, {date}, page {page_no}")
        return []

    event_details = []
    for event_id in event_ids:
        try:
            if details := fetch_event_details(event_id):
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
    logger.info(f"Fetched {len(event_details)} event details")
    return event_details

def write_all_event_details(**context):
    ti = context['ti']
    all_event_details = []

    for task_id in context['task'].upstream_task_ids:
        if details := ti.xcom_pull(task_ids=task_id, key='return_value'):
            all_event_details.extend(details)

    with open("event_details.json", "w") as f:
        json.dump(all_event_details, f, indent=2)

    logger.info(f"Wrote {len(all_event_details)} events to event_details.json")
    return all_event_details

def get_search_terms_from_db():
    logger.info("Fetching search terms from metadata DB...")
    handler = MetadataHandler(logger=logger)
    return handler.get_ingestions_to_attempt()


with DAG(
    dag_id="ingest_events_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # Entry point
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: logger.info("Starting ingest_events_dag"),
    )

    # Fetch search terms from metadata DB
    search_df = get_search_terms_from_db()
    region_ids = search_df["region_id"].unique().tolist()

    # Collect detail-fetch tasks to chain to write
    event_detail_tasks: List[PythonOperator] = []

    for region_id in region_ids:
        region_df = search_df[search_df["region_id"] == region_id]
        state_code = region_df["state_code"].iloc[0]
        city_code = region_df["city_code"].iloc[0]

        # Region-level task
        region_task = PythonOperator(
            task_id=f"{state_code}--{city_code}",
            python_callable=lambda: logger.info(f"Processing region {region_id}"),
            op_kwargs={"region_id": region_id},
            doc=f"Process region {region_id}",
        )
        start >> region_task

        # Event-type and date scoping
        for event_type in region_df["source_event_type_string"].unique():
            event_type_df = region_df[region_df["source_event_type_string"] == event_type]
            event_task = PythonOperator(
                task_id=f"{state_code}--{city_code}_{event_type}",
                python_callable=lambda: logger.info(f"Processing event type {event_type}"),
                op_kwargs={"event_type": event_type},
                doc=f"Process event type {event_type}",
            )
            region_task >> event_task

            for date in event_type_df["date"].unique():
                date_task = PythonOperator(
                    task_id=f"{state_code}--{city_code}_{event_type}_{date}",
                    python_callable=lambda: logger.info(f"Processing date {date}"),
                    op_kwargs={"date": date},
                    doc=f"Process date {date}",
                )
                event_task >> date_task

                # Paging through search results
                for page_no in range(1, PAGE_NOS):
                    common_kwargs = {
                        "event_type": event_type,
                        "state_code": state_code,
                        "city_code": city_code,
                        "date": date,
                        "page_no": page_no,
                    }
                    retrieve_task = PythonOperator(
                        task_id=f'retrieve_event_ids_{state_code}--{city_code}_{event_type}_{date}_{page_no}',
                        python_callable=retrieve_event_ids,
                        op_kwargs=common_kwargs,
                    )
                    fetch_task = PythonOperator(
                        task_id=f"fetch_{state_code}--{city_code}_{event_type}_{date}_{page_no}",
                        python_callable=fetch_details_for_event_ids,
                        op_kwargs=common_kwargs,
                    )
                    date_task >> retrieve_task >> fetch_task
                    event_detail_tasks.append(fetch_task)


    write_task = PythonOperator(
        task_id="write_all_event_details",
        python_callable=write_all_event_details
    )

    for task in event_detail_tasks:
        task >> write_task
