"""
this file contains code which gets the json formatting of song ids. run this after the recommendation model. 
"""

import json
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session):
    df = session.table("FINAL_RECOMMENDATIONS").select("SONG_ID")
    song_ids = [row["SONG_ID"] for row in df.collect()]
    
    json_output = json.dumps(song_ids, indent=4)
    print(json_output)
    return session.create_dataframe([[json_output]], schema=["SONG_IDS_JSON"])