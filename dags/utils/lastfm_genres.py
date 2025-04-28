import pandas as pd
import requests
import os
from airflow.models import Variable

def fetch_lastfm_genres(
    artists_csv_path="/opt/airflow/dags/data/artists.csv",
    output_csv_path="/opt/airflow/dags/data/genres.csv",
    api_key=None
):
    """
    Fetches genres for each artist from Last.fm API and saves them.
    """
    if api_key is None:
        api_key = Variable.get("LASTFM_API_KEY")

    artists_df = pd.read_csv(artists_csv_path)
    unique_artists = artists_df['artist'].dropna().unique()

    genres_data = []

    for artist in unique_artists:
        try:
            response = requests.get(
                "http://ws.audioscrobbler.com/2.0/",
                params={
                    "method": "artist.gettoptags",
                    "artist": artist,
                    "api_key": api_key,
                    "format": "json"
                }
            )
            data = response.json()

            tags = [tag['name'] for tag in data.get('toptags', {}).get('tag', [])]
            tags_str = ";".join(tags)

            genres_data.append({
                "artist": artist,
                "genres": tags_str
            })

        except Exception as e:
            print(f"Error fetching genres for artist '{artist}': {e}")
            genres_data.append({
                "artist": artist,
                "genres": ""
            })

    genres_df = pd.DataFrame(genres_data)
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    genres_df.to_csv(output_csv_path, index=False)

    print(f"Genres saved to {output_csv_path}")

def main():
    fetch_lastfm_genres(
        artists_csv_path="/home/warehouse/j.h.sun/airflow/dags/data/artists.csv",
        output_csv_path="/home/warehouse/j.h.sun/airflow/dags/data/genres.csv"
    )

if __name__ == "__main__":
    main()