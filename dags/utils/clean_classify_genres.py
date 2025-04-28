import pandas as pd
import re
from transformers import pipeline
from tqdm import tqdm
import os

def clean_and_classify_genres(input_path="/home/warehouse/j.h.sun/airflow/dags/data/genres.csv", output_path="/home/warehouse/j.h.sun/airflow/dags/data/genres_classified.csv"):
    """
    cleans raw genre strings and classifies them into major genre categories
    """

    # load data
    artists_genres_df = pd.read_csv(input_path)

    # clean and take only the first genre
    def extract_first_genre(genre_str):
        if pd.isna(genre_str):
            return None
        cleaned = re.sub(r"\[\d+\]", "", genre_str).lower()
        genres = [g.strip() for g in cleaned.split(",") if g.strip()]
        return genres[0] if genres else None

    artists_genres_df["main_genre"] = artists_genres_df["genres"].apply(extract_first_genre)

    # load model
    classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli", from_flax=True)

    # candidate labels
    candidate_labels = [
        "pop", "rock", "rap", "country", "indie", "folk",
        "metal", "jazz", "r&b", "electronic", "hip hop",
        "punk", "classical", "alternative"
    ]

    tqdm.pandas()

    unique_genres = (
        artists_genres_df[["main_genre"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # classify genres
    def classify_genre(g):
        try:
            result = classifier(f"{g} is a music genre.", candidate_labels)
            return result["labels"][0]
        except Exception as e:
            print(f"Error classifying genre '{g}': {e}")
            return None

    unique_genres["predicted_genre"] = unique_genres["main_genre"].progress_apply(classify_genre)

    # merge predictions back
    artists_genres_df = artists_genres_df.merge(unique_genres, on="main_genre", how="left")

    # save output
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    artists_genres_df.to_csv(output_path, index=False)

    print(f"Cleaned and classified genres saved to {output_path}")

def main():
    clean_and_classify_genres(
        input_path="/home/warehouse/j.h.sun/airflow/dags/data/genres.csv",
        output_path="/home/warehouse/j.h.sun/airflow/dags/data/genres_classified.csv"
    )

if __name__ == "__main__":
    main()