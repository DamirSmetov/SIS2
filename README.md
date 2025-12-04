
# iSpace ETL Pipeline

## Project Description
**iSpace** is an e-commerce website specializing in Apple products, including iPhones, iPads, MacBooks, and accessories.  
This project implements an **ETL (Extract, Transform, Load) pipeline** to extract product data from the dynamic iSpace website, clean and transform it, and store it in a SQLite database for further analysis.

The ETL pipeline is orchestrated using **Apache Airflow**, which allows scheduling, triggering, and monitoring of the scraping workflow. The parser runs automatically when the DAG is triggered.

---

## Installation and Setup

### 1. Clone Repository
```bash
git clone <your-repo-url>
cd <your-repo-folder>


### 2. Create Virtual Environment and Install Dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> Install Playwright browsers:

```bash
playwright install
```

### 3. Set Up Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow
mkdir -p $AIRFLOW_HOME
airflow db init
```

### 4. Start Airflow

```bash
airflow standalone
```

This will start the webserver, scheduler, and create an admin user. Access the Airflow UI at `http://localhost:8080`.

---

## Running the ETL Pipeline

1. Open Airflow UI.
2. Enable the DAG: `web_scrape`.
3. Click **Trigger DAG**.

> The scraper will extract product data from iSpace, clean it, and load it into the SQLite database automatically.

---

## Expected Output

* **SQLite Database**: Table with iPad products, including:

  * Model
  * Storage
  * Price
  * Other technical specifications
* **Optional Parquet/CSV files**: Raw and cleaned data stored in `data/` folder.

---

## Project Structure

```
SIS2/
│
├─ airflow/
│  ├─ dags/
│  │   └─ web_scrape/
│  │        ├─ dag.py
│  │        └─ tasks
│  ├─ airflow.db
│  └─ logs/
│
├─ data/                  # Output folder for parquet/CSV files
├─ .venv/                 # Python virtual environment
├─ requirements.txt       # Python dependencies
└─ README.md
```

---

## Notes

* Each DAG should live in its own folder with helper modules (e.g., data cleaning, loading scripts).
* Do **not** commit `airflow.db` or `logs/`.
* Airflow handles retries and logging; monitor execution via the UI.
