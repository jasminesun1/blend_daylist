import pandas as pd
import os

def split_data(filtered_data_path="/home/warehouse/j.h.sun/airflow/dags/data/filtered_data.csv", output_dir="/home/warehouse/j.h.sun/airflow/dags/data/"):
    """
    Splits filtered data into songs_incomplete, artists, listening_records
    """
    df = pd.read_csv(filtered_data_path)

    # SONGS INCOMPLETE
    songs_df = (
        df[['song_id', 'song', 'artist', 'album']]
        .drop_duplicates(subset=['song_id'])
        .dropna(subset=['song_id'])
    )
    songs_df.to_csv(os.path.join(output_dir, "songs_incomplete.csv"), index=False)

    # ARTISTS
    artists_df = (
        songs_df[['artist']]
        .drop_duplicates()
        .dropna()
    )
    artists_df.to_csv(os.path.join(output_dir, "artists_data.csv"), index=False)

    # LISTENING RECORDS
    listening_records_df = df[['timestamp', 'song_id', 'user', 'skipped']]
    listening_records_df.to_csv(os.path.join(output_dir, "listening_records.csv"), index=False)

    print(f"Split outputs saved to {output_dir}")

def main():
    split_data(
        filtered_data_path="/home/warehouse/j.h.sun/airflow/dags/data/filtered_data.csv",
        output_dir="/home/warehouse/j.h.sun/airflow/dags/data/"
    )

if __name__ == "__main__":
    main()