import os
import csv
import base64
import time
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException

ARTISTS_CSV     = os.path.join(os.path.dirname(__file__), "artists_ids.csv")
TARGET_TABLE    = "FERRET_DB.AIRFLOW.SONGS"
SPOTIFY_CONN_ID = "spotify_default"
SNOWFLAKE_CONN_ID = "ferretsnowflake"

default_args = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spotify_project_pipeline",
    default_args=default_args,
    schedule_interval="@weekly",
    start_date=datetime(2025, 3, 1),
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "snowflake"],
) as dag:

    def get_spotify_token(ti):
        """
        Fetch a Client-Credentials token from Spotify and push it to XCom.
        """
        hook = HttpHook(http_conn_id=SPOTIFY_CONN_ID, method="POST")
        conn = hook.get_connection(SPOTIFY_CONN_ID)
        creds = f"{conn.login}:{conn.password}"
        b64 = base64.b64encode(creds.encode()).decode()
        headers = {"Authorization": f"Basic {b64}"}
        data = {"grant_type": "client_credentials"}

        try:
            resp = hook.run(endpoint="/api/token", headers=headers, data=data, extra_options={"timeout": 10})
            resp.raise_for_status()
            token = resp.json()["access_token"]
            ti.xcom_push(key="spotify_token", value=token)
            logging.info("Retrieved Spotify token")
        except Exception as e:
            logging.error(f"Failed to retrieve Spotify token: {e}")
            raise AirflowException("Spotify token retrieval failed")

    def fetch_and_filter_new_releases(ti, **context):
        """
        For each artist, pull albums released in the last interval,
        then pull each album's tracks, assemble full record list.
        """
        ds_start = context["data_interval_start"].date().isoformat()
        ds_end = context["data_interval_end"].date().isoformat()
        logging.info(f"Looking for releases between {ds_start} and {ds_end}")

        token = ti.xcom_pull(task_ids="get_spotify_token", key="spotify_token")
        if not token:
            raise AirflowException("No Spotify token in XCom")

        with open(ARTISTS_CSV, newline="") as f:
            artists = [r["artist_id"] for r in csv.DictReader(f)]
        logging.info(f"Loaded {len(artists)} artists")

        hook = HttpHook(http_conn_id=SPOTIFY_CONN_ID, method="GET")
        headers = {"Authorization": f"Bearer {token}"}
        records = []

        for artist_id in artists:
            try:
                resp = hook.run(
                    endpoint=f"/v1/artists/{artist_id}/albums",
                    headers=headers,
                    extra_options={"params": {"include_groups": "album,single", "country": "US", "limit": 50}, "timeout": 10},
                )
                resp.raise_for_status()
                albums = resp.json().get("items", [])
            except Exception as e:
                logging.warning(f"Skipping artist {artist_id} (albums): {e}")
                continue

            window = [a for a in albums if ds_start <= a.get("release_date", "") < ds_end]

            for alb in window:
                album_id = alb["id"]
                album_name = alb["name"]
                rd = alb.get("release_date")

                try:
                    resp2 = hook.run(
                        endpoint=f"/v1/albums/{album_id}/tracks",
                        headers=headers,
                        extra_options={"params": {"limit": 50}, "timeout": 10},
                    )
                    resp2.raise_for_status()
                    tracks = resp2.json().get("items", [])
                except Exception as e:
                    logging.warning(f" → skipping tracks for {album_id}: {e}")
                    tracks = []

                for tr in tracks:
                    records.append({
                        "album": album_name,
                        "album_id": album_id,
                        "artist_id": int(artist_id),  
                        "duration_ms": tr.get("duration_ms"),
                        "explicit": tr.get("explicit"),
                        "is_listened_before": False,
                        "popularity": tr.get("popularity"),
                        "release_date": rd,
                        "song_id": tr["id"],
                        "song_name": tr["name"],
                    })

                time.sleep(0.1)

        if not records:
            logging.warning("No new tracks found to load — failing task.")
            raise AirflowException("No new releases found during fetch — aborting.")

        ti.xcom_push(key="new_releases", value=records)
        logging.info(f"→ Pushed {len(records)} track records")

    def load_to_snowflake(ti):
        """
        Read the XCom’d list of dicts and insert into Snowflake,
        ignoring duplicates on SONG_ID.
        """
        recs = ti.xcom_pull(task_ids="fetch_and_filter_new_releases", key="new_releases") or []
        if not recs:
            logging.info("No new tracks to load → skipping insert.")
            return

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for r in recs:
                    cur.execute(f"""
                        INSERT INTO {TARGET_TABLE}
                          (ALBUM, ALBUM_ID, ARTIST_ID, DURATION_MS,
                           EXPLICIT, IS_LISTENED_BEFORE, POPULARITY,
                           RELEASE_DATE, SONG_ID, SONG_NAME)
                        SELECT
                          %(album)s, %(album_id)s, %(artist_id)s,
                          %(duration_ms)s, %(explicit)s,
                          %(is_listened_before)s, %(popularity)s,
                          %(release_date)s, %(song_id)s, %(song_name)s
                        WHERE NOT EXISTS (
                          SELECT 1 FROM {TARGET_TABLE}
                           WHERE SONG_ID = %(song_id)s
                        )
                    """, r)
            conn.commit()
        logging.info(f"Loaded {len(recs)} new tracks into {TARGET_TABLE}")

    t1 = PythonOperator(
        task_id="get_spotify_token",
        python_callable=get_spotify_token,
    )

    t2 = PythonOperator(
        task_id="fetch_and_filter_new_releases",
        python_callable=fetch_and_filter_new_releases,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    t1 >> t2 >> t3
