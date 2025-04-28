"""
this file contains the code for the recommendation model. run it after create_model_features.sql
"""

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, when, approx_percentile, row_number
from snowflake.snowpark.window import Window
import random

def main(session: snowpark.Session):
    session.use_database("ferret_db")
    session.use_schema("airflow")

    time_of_day = "night" # morning, afternoon, night
    genre = "alternative" # electronic, alternative, folk, punk, pop, rock, rap, hip hop, country, classical, r&b, jazz, metal, indie
    discovery_mode = True # true --> 1/3 of the songs will be songs we have not heard
    underground_mode = False # true --> only recommends songs with below average popularity

    df = session.table("recommendation_features")
    df = preprocess(df, genre)

    if underground_mode:
        df = filter_by_underground_mode(df)

    df = score_songs(df.filter(col("IS_LISTENED_BEFORE") == True), time_of_day)
    recommended = recommend(df, session, discovery_mode)
    recommended.show()
    return recommended

def preprocess(df, genre):
    # drop heavily skipped songs
    df = df.filter(col("SKIPPED_RATIO") < 0.5)

    # filter by genre
    if genre is not None:
        df = df.filter(col("GENRE_NAME") == genre)

    # fill medians for null scores
    columns_to_fill = ["CLAIRE_SCORE", "JASMINE_SCORE", "SASHA_SCORE"]
    medians = {}
    for colname in columns_to_fill:
        median_val = df.agg(approx_percentile(col(colname), 0.5)).collect()[0][0]
        medians[colname] = median_val

    for colname in columns_to_fill:
        df = df.with_column(
            colname,
            when(col(colname).is_null(), medians[colname]).otherwise(col(colname))
        )

    return df


def filter_by_underground_mode(df):
    mean_popularity = df.agg({"POPULARITY": "avg"}).collect()[0][0]
    return df.filter(col("POPULARITY") < mean_popularity)

def score_songs(df, time_of_day):
    weights = {
        "morning":   [0.3, 0.05, 0.05, 0.2, 0.2, 0.2],
        "afternoon": [0.05, 0.3, 0.05, 0.2, 0.2, 0.2],
        "night":     [0.05, 0.05, 0.3, 0.2, 0.2, 0.2],
    }[time_of_day]

    return df.with_column(
        "SCORE",
        weights[0] * col("MORNING_RATIO") +
        weights[1] * col("AFTERNOON_RATIO") +
        weights[2] * col("NIGHT_RATIO") +
        weights[3] * col("CLAIRE_SCORE") +
        weights[4] * col("JASMINE_SCORE") +
        weights[5] * col("SASHA_SCORE")
    )

def recommend(scored_df, session, discovery_mode):
    from snowflake.snowpark.functions import row_number
    from snowflake.snowpark.window import Window

    # limit to 15 songs per artist
    window = Window.partition_by("ARTIST_ID").order_by(col("SCORE").desc())
    scored_with_row = scored_df.with_column("ROW_NUM", row_number().over(window))
    scored_limited = scored_with_row.filter(col("ROW_NUM") <= 15).drop("ROW_NUM")

    # take top 100 scores
    top_100_df = scored_limited.sort(col("SCORE").desc()).limit(100)
    top_100_ids = [row["SONG_ID"] for row in top_100_df.collect()]

    if discovery_mode:
        # sample 20 from top 100
        sample_20 = random.sample(top_100_ids, min(20, len(top_100_ids)))

        # get top artist ids from top 100
        artist_ids = [row["ARTIST_ID"] for row in top_100_df.select("ARTIST_ID").distinct().collect()]
        print("Top artist IDs in top 100:", artist_ids)

        # get 1000 new songs from those artists
        full_df = session.table("recommendation_features")
        discovery_df = (
            full_df
            .filter((col("IS_LISTENED_BEFORE") == False) & col("ARTIST_ID").isin(artist_ids))
            .limit(1000)
        )
        discovery_ids = [row["SONG_ID"] for row in discovery_df.collect()]

        # sample 10 new songs
        sample_10 = random.sample(discovery_ids, min(10, len(discovery_ids)))
        all_ids = sample_20 + sample_10

    else:
        # sample 30
        all_ids = random.sample(top_100_ids, min(30, len(top_100_ids)))

    # save the final ones
    final_df = session.table("recommendation_features").filter(col("SONG_ID").isin(all_ids))
    final_df.write.mode("overwrite").save_as_table("FINAL_RECOMMENDATIONS")

    return final_df




