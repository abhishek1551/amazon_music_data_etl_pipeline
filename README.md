# Amazon Music Data Scraper with Apache Airflow

This project automates the process of **scraping data from Amazon** (specifically about music) and **storing it in a PostgreSQL database**. The workflow is orchestrated using **Apache Airflow**, providing scalability, automation, and scheduling capabilities.
## Prerequisites
To run this project, ensure you have the following installed:
- **Docker** and **Docker Compose**: For containerized setup
- **PostgreSQL**: As the target database
- **Python 3.x**: If running outside of Docker for testing purposes
- **Apache Airflow**: For orchestration and task scheduling


## Key Features:

### 1. Data Extraction (Scraping)
The script uses the `requests` library to send HTTP requests to Amazon, fetching music data. The `BeautifulSoup` library is used to parse and extract relevant information from the HTML content.

### 2. Data Transformation
The scraped data is cleaned and processed using **Pandas**. Duplicates are removed, and the data is organized into a DataFrame for easy manipulation.

### 3. Data Loading (PostgreSQL)
The cleaned data is inserted into a **PostgreSQL database** using **PostgresHook** and **PostgresOperator** in Airflow. This allows for persistent storage of the scraped data.

### 4. Apache Airflow Orchestration
The entire workflow is automated and managed using **Apache Airflow**. Airflow is used to schedule, monitor, and orchestrate tasks in a Directed Acyclic Graph (DAG). The DAG consists of tasks such as:
- Fetching data from Amazon
- Cleaning the data
- Storing the data in a PostgreSQL table
- Ensuring the tasks are executed in the correct order and managing dependencies.

### 5. Scalability and Scheduling
Airflow enables the project to be scheduled to run at regular intervals (e.g., daily) and allows easy scaling for larger datasets or additional tasks (such as scraping other product categories).




