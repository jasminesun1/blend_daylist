from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

import pandas as pd
import snowflake.connector
import os

from utils import (
    initial_cleaning,
    split_data,
    split_chunks,
    lastfm_genres,
    clean_classify_genres,
    fetch_spotify_metadata
)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# replace below with your connection information
SNOWFLAKE_CONN_PARAMS = {
    "user": "",
    "password": "", 
    "account": "",
    "warehouse": "",
    "database": "",
    "schema": ""
}

def upload_csv_to_stage(file_path, stage_name):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONN_PARAMS)
    cursor = conn.cursor()
    put_command = f"PUT file://{file_path} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    cursor.execute(put_command)
    cursor.close()
    conn.close()

with DAG(
    dag_id='music_data_pipeline_full',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # initial cleaning
    initial_cleaning_task = PythonOperator(
        task_id="initial_cleaning",
        python_callable=initial_cleaning.main
    )

    # split data into tables
    split_data_task = PythonOperator(
        task_id="split_data",
        python_callable=split_data.main
    )

    # split into chunks for faster data collection
    split_chunks_task = PythonOperator(
        task_id="split_chunks",
        python_callable=split_chunks.main
    )

    # get genre data from lastFM
    lastfm_genres_task = PythonOperator(
        task_id="lastfm_genres",
        python_callable=lastfm_genres.main
    )

    # clean/classify genres
    clean_classify_genres_task = PythonOperator(
        task_id="clean_classify_genres",
        python_callable=clean_classify_genres.main
    )

    # get spotify data
    fetch_spotify_task = PythonOperator(
        task_id="fetch_spotify_metadata",
        python_callable=fetch_spotify_metadata.main
    )

    # upload csvs to stages
    upload_artists_to_stage = PythonOperator(
        task_id="upload_artists_to_stage",
        python_callable=upload_csv_to_stage,
        op_kwargs={"file_path": "/home/warehouse/j.h.sun/airflow/dags/data/artists_data.csv", "stage_name": "ARTISTS_STAGE"}
    )

    upload_listening_records_to_stage = PythonOperator(
        task_id="upload_listening_records_to_stage",
        python_callable=upload_csv_to_stage,
        op_kwargs={"file_path": "/home/warehouse/j.h.sun/airflow/dags/data/listening_records.csv", "stage_name": "LISTENING_RECORDS_STAGE"}
    )

    upload_genres_to_stage = PythonOperator(
        task_id="upload_genres_to_stage",
        python_callable=upload_csv_to_stage,
        op_kwargs={"file_path": "/home/warehouse/j.h.sun/airflow/dags/data/genres_classified.csv", "stage_name": "GENRES_STAGE"}
    )

    upload_songs_incomplete_to_stage = PythonOperator(
        task_id="upload_songs_incomplete_to_stage",
        python_callable=upload_csv_to_stage,
        op_kwargs={"file_path": "/home/warehouse/j.h.sun/airflow/dags/data/songs_incomplete.csv", "stage_name": "SONGS_INCOMPLETE_STAGE"}
    )

    upload_expanded_song_dataset_to_stage = PythonOperator(
        task_id="upload_expanded_song_dataset_to_stage",
        python_callable=upload_csv_to_stage,
        op_kwargs={"file_path": "/home/warehouse/j.h.sun/airflow/dags/data/expanded_song_dataset.csv", "stage_name": "EXPANDED_SONG_DATASET_STAGE"}
    )

    # copy into snowflake tables
    copy_artists = SnowflakeOperator(
        task_id="copy_artists",
        sql="COPY INTO ARTISTS FROM @ARTISTS_STAGE FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='\"')",
        snowflake_conn_id="snowflake"
    )

    copy_listening_records = SnowflakeOperator(
        task_id="copy_listening_records",
        sql="COPY INTO LISTENING_RECORDS FROM @LISTENING_RECORDS_STAGE FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='\"')",
        snowflake_conn_id="snowflake"
    )

    copy_genres = SnowflakeOperator(
        task_id="copy_genres",
        sql="COPY INTO GENRES FROM @GENRES_STAGE FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='\"')",
        snowflake_conn_id="snowflake"
    )

    copy_songs_incomplete = SnowflakeOperator(
        task_id="copy_songs_incomplete",
        sql="COPY INTO SONGS_INCOMPLETE FROM @SONGS_INCOMPLETE_STAGE FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='\"')",
        snowflake_conn_id="snowflake"
    )

    copy_expanded_song_dataset = SnowflakeOperator(
        task_id="copy_expanded_song_dataset",
        sql="COPY INTO EXPANDED_SONG_DATASET FROM @EXPANDED_SONG_DATASET_STAGE FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='\"')",
        snowflake_conn_id="snowflake"
    )

    # create final songs table
    with open("/home/warehouse/j.h.sun/airflow/dags/sql/create_songs.sql", "r") as file:
        create_songs_sql = file.read()

    create_songs_table = SnowflakeOperator(
        task_id='create_songs_table',
        sql=create_songs_sql,
        snowflake_conn_id="snowflake"
    )

    # dag flow
    (
        initial_cleaning_task
        >> split_data_task
        >> split_chunks_task
        >> lastfm_genres_task
        >> clean_classify_genres_task
        >> fetch_spotify_task
        >> [upload_artists_to_stage, upload_listening_records_to_stage, upload_genres_to_stage, upload_songs_incomplete_to_stage, upload_expanded_song_dataset_to_stage]
    )

    upload_artists_to_stage >> copy_artists
    upload_listening_records_to_stage >> copy_listening_records
    upload_genres_to_stage >> copy_genres
    upload_songs_incomplete_to_stage >> copy_songs_incomplete
    upload_expanded_song_dataset_to_stage >> copy_expanded_song_dataset

    [copy_artists, copy_listening_records, copy_genres, copy_songs_incomplete, copy_expanded_song_dataset] >> create_songs_table