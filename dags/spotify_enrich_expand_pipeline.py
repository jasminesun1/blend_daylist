import os
import pandas as pd
import json
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.sql import SqlSensor
from datetime import datetime

import snowflake.connector
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

SPOTIFY_CLIENT_ID = '669750ef3654483baa921fba97845d1c'
SPOTIFY_CLIENT_SECRET = '5be9f0c1a9c84b1d8df42d42788055b9'

auth_manager = SpotifyClientCredentials(client_id=SPOTIFY_CLIENT_ID, client_secret=SPOTIFY_CLIENT_SECRET)
sp = spotipy.Spotify(auth_manager=auth_manager)

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

def format_track(track_obj):
    return {
        'track_name': track_obj['name'],
        'track_id': track_obj['id'],
        'artist': track_obj['artists'][0]['name'],
        'album': track_obj['album']['name'],
        'album_id': track_obj['album']['id'],
        'release_date': track_obj['album']['release_date'],
        'popularity': track_obj.get('popularity'),
        'explicit': track_obj.get('explicit'),
        'duration_ms': track_obj.get('duration_ms'),
        'preview_url': track_obj.get('preview_url')
    }

def batch_track_info(track_ids, batch_size=50):
    all_data = []

    for i in range(0, len(track_ids), batch_size):
        batch = track_ids[i:i+batch_size]
        while True:
            try:
                results = sp.tracks(batch)
                break
            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429:
                    retry_after = int(e.headers.get('Retry-After', 5))
                    print(f"Rate limited. Sleeping for {retry_after} seconds...")
                    time.sleep(retry_after)
                else:
                    print(f"Unexpected error: {e}")
                    break

        for track in results['tracks']:
            if track:
                all_data.append(format_track(track))

    return pd.DataFrame(all_data)


def read_existing_data(**kwargs):
    details = get_snowflake_connection()
    conn = snowflake.connector.connect(**details)

    songs_df = pd.read_sql(f"SELECT * FROM {details['database']}.{details['schema']}.SONGS_DATA", conn)
    artists_df = pd.read_sql(f"SELECT * FROM {details['database']}.{details['schema']}.ARTISTS_DATA", conn)

    listening_query = f"""
    SELECT 
        l.TIMESTAMP,
        l.SONG_ID,
        l.SKIPPED,
        l.USER,
        a.ARTIST_NAME AS ARTIST
    FROM {details['database']}.{details['schema']}.LISTENING_DATA l
    JOIN {details['database']}.{details['schema']}.SONGS_DATA s ON l.SONG_ID = s.SONG_ID
    JOIN {details['database']}.{details['schema']}.ARTISTS_DATA a ON s.ARTIST_ID = a.ARTIST_ID
    """

    listening_df = pd.read_sql(listening_query, conn)

    conn.close()

 
    songs_df.columns = [col.lower() for col in songs_df.columns]
    artists_df.columns = [col.lower() for col in artists_df.columns]
    listening_df.columns = [col.lower() for col in listening_df.columns]

    kwargs['ti'].xcom_push(key='songs_data', value=songs_df.to_json())
    kwargs['ti'].xcom_push(key='listening_data', value=listening_df.to_json())
    kwargs['ti'].xcom_push(key='artists_data', value=artists_df.to_json())



def enrich_existing_songs(**kwargs):
    ti = kwargs['ti']
    songs_json = ti.xcom_pull(task_ids='read_existing_data', key='songs_data')
    songs_df = pd.read_json(songs_json)

    track_ids = songs_df['song_id'].dropna().unique().tolist()
    enriched_tracks = batch_track_info(track_ids)

    ti.xcom_push(key='enriched_songs', value=enriched_tracks.to_json())

from concurrent.futures import ThreadPoolExecutor, as_completed



def build_top_artist_lists(**kwargs):
    ti = kwargs['ti']
    listening_json = ti.xcom_pull(task_ids='read_existing_data', key='listening_data')
    listening_df = pd.read_json(listening_json)


    top_artists_per_user = (
        listening_df.groupby(['user', 'artist'])
        .size()
        .reset_index(name='play_count')
        .sort_values(['user', 'play_count'], ascending=[True, False])
        .groupby('user')
        .head(20)
    )

    top_listened_artists_total = top_artists_per_user['artist'].drop_duplicates().tolist()

    artists_with_over_50 = (
        listening_df.groupby('artist')
        .size()
        .reset_index(name='play_count')
        .query('play_count > 50')
    )

    artists_with_over_50 = artists_with_over_50[
        ~artists_with_over_50['artist'].isin(top_listened_artists_total)
    ]['artist'].tolist()

    ti.xcom_push(key='top_listened_artists_total', value=top_listened_artists_total)
    ti.xcom_push(key='artists_with_over_50', value=artists_with_over_50)


def expand_by_album_tracks(**kwargs):
    ti = kwargs['ti']
    top_artists = ti.xcom_pull(task_ids='build_top_artist_lists', key='top_listened_artists_total')

    all_track_ids = set()

    for artist in top_artists:
        try:
            results = sp.search(q=f'artist:{artist}', type='artist', limit=1)
            items = results['artists']['items']
            if not items:
                continue
            artist_id = items[0]['id']

            albums = sp.artist_albums(artist_id, album_type='album,single', limit=50)['items']
            for album in albums:
                tracks = sp.album_tracks(album['id'])['items']
                for track in tracks:
                    all_track_ids.add(track['id'])
        except Exception as e:
            print(f"Error processing artist {artist}: {e}")

    expanded_tracks_df = batch_track_info(list(all_track_ids))

    ti.xcom_push(key='expanded_album_tracks', value=expanded_tracks_df.to_json())

def expand_by_top_songs(**kwargs):
    ti = kwargs['ti']
    artists_with_over_50 = ti.xcom_pull(task_ids='build_top_artist_lists', key='artists_with_over_50')

    all_tracks = []

    for artist in artists_with_over_50:
        try:
            results = sp.search(q=f'artist:{artist}', type='artist', limit=1)
            items = results['artists']['items']
            if not items:
                continue
            artist_id = items[0]['id']

            top_tracks = sp.artist_top_tracks(artist_id, country='US')['tracks']
            for track in top_tracks:
                all_tracks.append(track['id'])
        except Exception as e:
            print(f"Error processing artist {artist}: {e}")

    expanded_top_songs_df = batch_track_info(all_tracks)

    ti.xcom_push(key='expanded_top_songs', value=expanded_top_songs_df.to_json())

def merge_and_upload_final_songs(**kwargs):
    ti = kwargs['ti']

    enriched_json = ti.xcom_pull(task_ids='enrich_existing_songs', key='enriched_songs')
    expanded_album_json = ti.xcom_pull(task_ids='expand_by_album_tracks', key='expanded_album_tracks')
    expanded_top_json = ti.xcom_pull(task_ids='expand_by_top_songs', key='expanded_top_songs')

    enriched_df = pd.read_json(enriched_json)
    expanded_album_df = pd.read_json(expanded_album_json)
    expanded_top_df = pd.read_json(expanded_top_json)

    final_df = pd.concat([enriched_df, expanded_album_df, expanded_top_df]).drop_duplicates(subset=['track_id'])

    details = get_snowflake_connection()
    conn = snowflake.connector.connect(**details)
    cur = conn.cursor()

    cur.execute(f"""
    CREATE OR REPLACE TABLE {details['database']}.{details['schema']}.SONGS_DATA_ENRICHED (
        TRACK_ID VARCHAR,
        TRACK_NAME VARCHAR,
        ARTIST VARCHAR,
        ALBUM VARCHAR,
        ALBUM_ID VARCHAR,
        RELEASE_DATE DATE,
        POPULARITY NUMBER,
        EXPLICIT BOOLEAN,
        DURATION_MS NUMBER,
        PREVIEW_URL VARCHAR
    );
    """)

    batch_size = 1000
    batch = []

    def batch_insert(rows):
        if not rows:
            return
        placeholders = ",".join(["(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"] * len(rows))
        flat = [item for row in rows for item in row]
        insert_query = f"""
            INSERT INTO {details['database']}.{details['schema']}.SONGS_DATA_ENRICHED 
            (TRACK_ID, TRACK_NAME, ARTIST, ALBUM, ALBUM_ID, RELEASE_DATE, POPULARITY, EXPLICIT, DURATION_MS, PREVIEW_URL)
            VALUES {placeholders}
        """
        cur.execute(insert_query, flat)

    def safe_value(v):
        return None if pd.isna(v) else v

    def fix_release_date(date_str):
        if pd.isna(date_str):
            return None
        parts = str(date_str).split('-')
        if len(parts) == 1:
            return f"{parts[0]}-01-01"  
        elif len(parts) == 2:
            return f"{parts[0]}-{parts[1]}-01" 
        else:
            return date_str  # Full

    for _, row in final_df.iterrows():
        batch.append((
            safe_value(row['track_id']),
            safe_value(row['track_name']),
            safe_value(row['artist']),
            safe_value(row['album']),
            safe_value(row['album_id']),
            safe_value(fix_release_date(row['release_date'])), 
            safe_value(row['popularity']),
            safe_value(row['explicit']),
            safe_value(row['duration_ms']),
            safe_value(row['preview_url'])
        ))
        if len(batch) >= batch_size:
            batch_insert(batch)
            batch = []

    if batch:
        batch_insert(batch)



    conn.commit()
    cur.close()
    conn.close()


with DAG(
    dag_id='spotify_enrich_expand_pipeline',
    start_date=datetime(2024, 4, 28),
    schedule_interval=None,
    catchup=False,
    tags=['spotify', 'api', 'enrichment'],
) as dag:

    wait_for_load_status = SqlSensor(
        task_id='wait_for_load_status',
        conn_id='Snowflake_Connection',
        sql="SELECT 1 FROM FERRET_DB.PIPELINE.LOAD_STATUS WHERE STATUS = 'COMPLETE'",
        timeout=600,
        poke_interval=30,
        mode='poke'
    )


    read_existing_data_task = PythonOperator(
        task_id='read_existing_data',
        python_callable=read_existing_data,
        provide_context=True
    )

    enrich_existing_songs_task = PythonOperator(
        task_id='enrich_existing_songs',
        python_callable=enrich_existing_songs,
        provide_context=True
    )

    build_top_artist_lists_task = PythonOperator(
        task_id='build_top_artist_lists',
        python_callable=build_top_artist_lists,
        provide_context=True
    )

    expand_by_album_tracks_task = PythonOperator(
        task_id='expand_by_album_tracks',
        python_callable=expand_by_album_tracks,
        provide_context=True
    )

    expand_by_top_songs_task = PythonOperator(
        task_id='expand_by_top_songs',
        python_callable=expand_by_top_songs,
        provide_context=True
    )

    merge_and_upload_final_songs_task = PythonOperator(
        task_id='merge_and_upload_final_songs',
        python_callable=merge_and_upload_final_songs,
        provide_context=True
    )

    wait_for_load_status >> read_existing_data_task
    read_existing_data_task >> [enrich_existing_songs_task, build_top_artist_lists_task]
    build_top_artist_lists_task >> [expand_by_album_tracks_task, expand_by_top_songs_task]
    [enrich_existing_songs_task, expand_by_album_tracks_task, expand_by_top_songs_task] >> merge_and_upload_final_songs_task
