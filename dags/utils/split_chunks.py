import pandas as pd
import os

def main():
    BASE_PATH = "/home/warehouse/j.h.sun/airflow/dags/data"
    SONGS_FILE = os.path.join(BASE_PATH, "artists_data.csv")
    CHUNKS_DIR = os.path.join(BASE_PATH, "artists_chunks")
    CHUNK_SIZE = 15

    # create chunks dir
    os.makedirs(CHUNKS_DIR, exist_ok=True)

    # load full
    df = pd.read_csv(SONGS_FILE)

    # split
    for i, start in enumerate(range(0, len(df), CHUNK_SIZE)):
        chunk = df.iloc[start:start + CHUNK_SIZE]
        chunk_path = os.path.join(CHUNKS_DIR, f"chunk_{i}.csv")
        chunk.to_csv(chunk_path, index=False)

    print(f"Split into {i + 1} chunks of {CHUNK_SIZE} rows each.")