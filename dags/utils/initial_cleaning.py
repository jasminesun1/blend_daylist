import numpy as np
import pandas as pd
import os
import glob
import json

def initial_cleaning(base_path="/opt/airflow/dags/data/", output_path="/opt/airflow/dags/data/filtered_data.csv"):
    """
    Cleans Spotify Extended Streaming Histories exactly like the provided Colab notebook.
    """
    subdirs = [os.path.join(base_path, d) for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]
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

    to_keep = [
        'ts',
        'master_metadata_track_name',
        'master_metadata_album_artist_name',
        'master_metadata_album_album_name',
        'spotify_track_uri',
        'skipped',
        'user'
    ]
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

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    filtered_df.to_csv(output_path, index=False)
    print(f"Filtered cleaned data saved to {output_path}")

def main():
    initial_cleaning(
        base_path="/home/warehouse/j.h.sun/airflow/dags/data/",
        output_path="/home/warehouse/j.h.sun/airflow/dags/data/filtered_data.csv"
    )

if __name__ == "__main__":
    main()