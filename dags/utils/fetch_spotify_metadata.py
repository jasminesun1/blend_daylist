import pandas as pd
import os
import requests
import time
from airflow.models import Variable

def fetch_spotify_metadata(
    songs_incomplete_path="/home/warehouse/j.h.sun/airflow/dags/data/songs_incomplete.csv",
    output_path="/home/warehouse/j.h.sun/airflow/dags/data/expanded_song_dataset.csv",
    client_id=None,
    client_secret=None
):
    """
    fetches Spotify metadata for additional song info and saves expanded song dataset
    """
    if client_id is None:
        client_id = Variable.get("SPOTIFY_CLIENT_ID")
    if client_secret is None:
        client_secret = Variable.get("SPOTIFY_CLIENT_SECRET")

    # authenticate
    auth_response = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret)
    )
    if auth_response.status_code != 200:
        raise Exception("Failed to authenticate with Spotify API.")
    access_token = auth_response.json()["access_token"]
    headers = {"Authorization": f"Bearer {access_token}"}

    # load incomplete songs
    songs_df = pd.read_csv(songs_incomplete_path)

    # prepare new data
    expanded_data = []

    for idx, row in songs_df.iterrows():
        track_id = row["song_id"]

        try:
            r = requests.get(f"https://api.spotify.com/v1/tracks/{track_id}", headers=headers)
            if r.status_code == 200:
                track_info = r.json()
                expanded_data.append({
                    "song_id": track_id,
                    "song": track_info["name"],
                    "artist": track_info["artists"][0]["name"],
                    "album": track_info["album"]["name"],
                    "duration_ms": track_info["duration_ms"],
                    "popularity": track_info["popularity"]
                })
            else:
                print(f"Track {track_id} not found (status {r.status_code}).")

        except Exception as e:
            print(f"Error fetching track {track_id}: {e}")

        time.sleep(0.1)

    # save expanded data
    expanded_df = pd.DataFrame(expanded_data)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    expanded_df.to_csv(output_path, index=False)

    print(f"Expanded song dataset saved to {output_path}")

def main():
    fetch_spotify_metadata(
        songs_incomplete_path="/home/warehouse/j.h.sun/airflow/dags/data/songs_incomplete.csv",
        output_path="/home/warehouse/j.h.sun/airflow/dags/data/expanded_song_dataset.csv"
    )

if __name__ == "__main__":
    main()
