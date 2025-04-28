import os
import glob
import json
import pandas as pd

import snowflake.connector

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime


def get_snowflake_connection():
    conn = BaseHook.get_connection("Snowflake_Connection")
    extras = conn.extra_dejson
    return {
        "account": extras.get("account"),
        "user": conn.login,
        "password": conn.password,
        "database": extras.get("database"),
        "schema": conn.schema,
        "warehouse": extras.get("warehouse"),
        "role": extras.get("role"),
        "insecure_mode": extras.get("insecure_mode", False)
    }


def load_and_clean_spotify_data(**kwargs):
    LOCAL_DATA_PATH = '/home/warehouse/c.skatrud/airflow/listening_data'

    subdirs = [os.path.join(LOCAL_DATA_PATH, d) for d in os.listdir(LOCAL_DATA_PATH) if os.path.isdir(os.path.join(LOCAL_DATA_PATH, d))]
    dfs = []

    for subdir in subdirs:
        json_files = glob.glob(os.path.join(subdir, '*.json'))
        folder_name = os.path.basename(subdir)
        user_name = folder_name.split()[-1]

        for file in json_files:
            filename = os.path.basename(file).lower()
            if 'audio' not in filename:
                continue
            try:
                with open(file, 'r') as f:
                    data = json.load(f)

                df = pd.DataFrame(data)
                df['user'] = user_name
                dfs.append(df)

            except Exception as e:
                print(f"Error reading {file}: {e}")

    combined_df = pd.concat(dfs, ignore_index=True)


    to_keep = ['ts', 'master_metadata_track_name', 'master_metadata_album_artist_name', 'master_metadata_album_album_name',
               'spotify_track_uri', 'skipped', 'user']
    filtered_df = combined_df.loc[:, combined_df.columns.isin(to_keep)]

    filtered_df = filtered_df.dropna(subset=['spotify_track_uri'])
    filtered_df['spotify_track_uri'] = filtered_df['spotify_track_uri'].str.replace('spotify:track:', '', regex=False)
    filtered_df['timestamp'] = pd.to_datetime(filtered_df.pop('ts'), utc=True)

    filtered_df = filtered_df.rename(columns={
        'master_metadata_track_name': 'song',
        'master_metadata_album_album_name': 'album',
        'master_metadata_album_artist_name': 'artist',
        'spotify_track_uri': 'song_id'
    })


    songs_df = (
        filtered_df[['song_id', 'song', 'artist', 'album']]
        .drop_duplicates(subset=['song_id'])
        .reset_index(drop=True)
    )

    # Listening
    listening_df = filtered_df.drop(['song', 'artist', 'album'], axis=1)

    # Artists
    artists_df = (
        filtered_df[['artist']]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
        .rename(columns={'artist': 'artist_name'})
    )
    artists_df['artist_id'] = artists_df.index

    songs_df = (
        filtered_df[['song_id', 'song', 'artist', 'album']]
        .drop_duplicates(subset=['song_id'])
        .merge(artists_df, left_on='artist', right_on='artist_name', how='left')
        .drop(columns=['artist', 'artist_name'])
        .reset_index(drop=True)
    )


    details = get_snowflake_connection()
    conn = snowflake.connector.connect(
        user=details['user'],
        password=details['password'],
        account=details['account'],
        warehouse=details['warehouse'],
        database=details['database'],
        schema=details['schema'],
        role=details['role'],
        insecure_mode=details['insecure_mode']
    )
    cur = conn.cursor()


    cur.execute(f"""
    CREATE OR REPLACE TABLE {details['database']}.{details['schema']}.FILTERED_DATA (
        TIMESTAMP TIMESTAMP_TZ,
        SONG_ID VARCHAR,
        SKIPPED BOOLEAN,
        USER VARCHAR
    );
    """)
    cur.execute(f"""
    CREATE OR REPLACE TABLE {details['database']}.{details['schema']}.SONGS_DATA (
        SONG_ID VARCHAR,
        SONG VARCHAR,
        ALBUM VARCHAR,
        ARTIST_ID NUMBER
    );
    """)
    cur.execute(f"""
    CREATE OR REPLACE TABLE {details['database']}.{details['schema']}.LISTENING_DATA (
        TIMESTAMP TIMESTAMP_TZ,
        SONG_ID VARCHAR,
        SKIPPED BOOLEAN,
        USER VARCHAR
    );
    """)
    cur.execute(f"""
    CREATE OR REPLACE TABLE {details['database']}.{details['schema']}.ARTISTS_DATA (
        ARTIST_NAME VARCHAR,
        ARTIST_ID NUMBER
    );
    """)


    batch_size = 1000

    def batch_insert(table_name, columns, rows):
        if not rows:
            return
        placeholders = ",".join(["(" + ",".join(["%s"] * len(columns)) + ")"] * len(rows))
        flat_values = [item for row in rows for item in row]
        insert_query = f"""
            INSERT INTO {details['database']}.{details['schema']}.{table_name} ({",".join(columns)})
            VALUES {placeholders}
        """
        cur.execute(insert_query, flat_values)

 
    batch = []
    for i, row in filtered_df.iterrows():
        batch.append((
            row['timestamp'].isoformat() if pd.notna(row['timestamp']) else None,
            row['song_id'],
            row['skipped'],
            row['user']
        ))
        if len(batch) >= batch_size:
            batch_insert('FILTERED_DATA', ['TIMESTAMP', 'SONG_ID', 'SKIPPED', 'USER'], batch)
            batch = []
    if batch:
        batch_insert('FILTERED_DATA', ['TIMESTAMP', 'SONG_ID', 'SKIPPED', 'USER'], batch)


    batch = []
    for i, row in songs_df.iterrows():
        batch.append((
            row['song_id'],
            row['song'],
            row['album'],
            int(row['artist_id']) if pd.notna(row['artist_id']) else None
        ))
        if len(batch) >= batch_size:
            batch_insert('SONGS_DATA', ['SONG_ID', 'SONG', 'ALBUM', 'ARTIST_ID'], batch)
            batch = []
    if batch:
        batch_insert('SONGS_DATA', ['SONG_ID', 'SONG', 'ALBUM', 'ARTIST_ID'], batch)


    batch = []
    for i, row in listening_df.iterrows():
        batch.append((
            row['timestamp'].isoformat() if pd.notna(row['timestamp']) else None,
            row['song_id'],
            row['skipped'],
            row['user']
        ))
        if len(batch) >= batch_size:
            batch_insert('LISTENING_DATA', ['TIMESTAMP', 'SONG_ID', 'SKIPPED', 'USER'], batch)
            batch = []
    if batch:
        batch_insert('LISTENING_DATA', ['TIMESTAMP', 'SONG_ID', 'SKIPPED', 'USER'], batch)


    batch = []
    for i, row in artists_df.iterrows():
        batch.append((
            row['artist_name'],
            int(row['artist_id'])
        ))
        if len(batch) >= batch_size:
            batch_insert('ARTISTS_DATA', ['ARTIST_NAME', 'ARTIST_ID'], batch)
            batch = []
    if batch:
        batch_insert('ARTISTS_DATA', ['ARTIST_NAME', 'ARTIST_ID'], batch)

    conn.commit()
    

    cur.execute(f"""
        CREATE OR REPLACE TABLE {details['database']}.{details['schema']}.LOAD_STATUS (
            STATUS VARCHAR,
            LOAD_TIMESTAMP TIMESTAMP_TZ
        );
    """)
    cur.execute(f"""
        INSERT INTO {details['database']}.{details['schema']}.LOAD_STATUS (STATUS, LOAD_TIMESTAMP)
        VALUES ('COMPLETE', CURRENT_TIMESTAMP())
    """)
    conn.commit()

    cur.close()
    conn.close()


with DAG(
    dag_id='spotify_load_clean_pipeline',
    start_date=datetime(2024, 4, 28),
    schedule_interval=None,
    catchup=False,
    tags=['spotify', 'snowflake', 'etl'],
) as dag:

    load_clean_spotify_task = PythonOperator(
        task_id='load_and_clean_spotify_data_task',
        python_callable=load_and_clean_spotify_data,
        provide_context=True
    )

    load_clean_spotify_task
