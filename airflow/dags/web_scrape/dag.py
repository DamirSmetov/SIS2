from airflow import DAG
from pathlib import Path
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import logging

FILE_DIR = Path(__file__).resolve().parent
if str(FILE_DIR) not in sys.path:
    sys.path.append(str(FILE_DIR))


def scrape_main_sync():
    import asyncio
    from scraper import main
    asyncio.run(main())

def clean_main_sync():
    from cleaner import main
    main()

def load_main_sync():
    from loader import main
    main()


with DAG(
    dag_id ="web_scrape",
    start_date= datetime.now(),
    schedule="0 1 * * *",
    catchup=False,
    description="Scrape → Clean → Load iSpace products",    
) as dag:
    scrape_task = PythonOperator(
        task_id="scrape_ispace",
        python_callable=scrape_main_sync,
    )
    clean_task = PythonOperator(
        task_id="clean_ispace_data",
        python_callable=clean_main_sync,
    )
    load_task = PythonOperator(
        task_id="load_ispace_data",
        python_callable=load_main_sync,
    )
scrape_task >> clean_task >> load_task
