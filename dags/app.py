from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "Windows",
    'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
}



# Function to fetch music album data
def get_amazon_data_music(num_albums, ti):
    base_url = f"https://www.amazon.com/s?k=music+albums"
    albums = []
    seen_titles = set()
    page = 1

    while len(albums) < num_albums:
        url = f"{base_url}&page={page}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            album_containers = soup.find_all("div", {"class": "s-result-item"})

            for album in album_containers:
                title = album.find("span", {"class": "a-text-normal"})
                artist = album.find("a", {"class": "a-size-base"})
                price = album.find("span", {"class": "a-price-whole"})
                release_date = album.find("span", {"class": "a-color-secondary"})
                rating = album.find("span", {"class": "a-icon-alt"})

                if title and artist and price and rating:
                    album_title = title.text.strip()
                    if album_title not in seen_titles:
                        seen_titles.add(album_title)
                        albums.append({
                            "Title": album_title,
                            "Artist": artist.text.strip(),
                            "Price": price.text.strip(),
                            "ReleaseDate": release_date.text.strip() if release_date else "Unknown",
                            "Rating": rating.text.strip(),
                        })

            page += 1
        else:
            print("Failed to retrieve the page")
            break

    albums = albums[:num_albums]
    df = pd.DataFrame(albums)
    df.drop_duplicates(subset="Title", inplace=True)
    ti.xcom_push(key='album_data', value=df.to_dict('records'))

# Function to insert data into PostgreSQL
def insert_album_data_into_postgres(ti):
    album_data = ti.xcom_pull(key='album_data', task_ids='fetch_music_data')
    if not album_data:
        raise ValueError("No album data found")

    postgres_hook = PostgresHook(postgres_conn_id='music_connection')
    insert_query = """
    INSERT INTO albums (title, artist, price, release_date, rating)
    VALUES (%s, %s, %s, %s, %s)
    """
    for album in album_data:
        postgres_hook.run(insert_query, parameters=(album['Title'], album['Artist'], album['Price'], album['ReleaseDate'], album['Rating']))

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'fetch_and_store_music_albums',
    default_args=default_args,
    description='DAG to fetch music album data from Amazon and store it in Postgres',
    schedule_interval=timedelta(days=1),
)

# Define the tasks
fetch_music_data_task = PythonOperator(
    task_id='fetch_music_data',
    python_callable=get_amazon_data_music,
    op_args=[50],  # Number of albums to fetch
    provide_context= True,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='music_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS albums (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        artist TEXT,
        price TEXT,
        release_date TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)

insert_music_data_task = PythonOperator(
    task_id='insert_music_data',
    python_callable=insert_album_data_into_postgres,
    dag=dag,
)

# Set task dependencies
create_table_task >> fetch_music_data_task >> insert_music_data_task
