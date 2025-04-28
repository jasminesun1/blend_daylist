# Blend Daylist

Below outlines instructions for running our code. 

In order to run our code, you will need a Snowflake account, a Snowflake connection through Airflow, LastFM API key, and Spotify API key and secret. The API keys will need to be placed in Airflow under Admin/Variables in the Airflow UI. 

Airflow requirements beyond what is already installed in the WashU provided environment:
* install transformers (pip install transformers)
* install torch (pip install torch)

### Step 1:
Run the create_stages.sql file in snowflake_worksheets. This will prepare your warehouse for data uploads from our DAGs. 

### Step 2:
Clean and augment our original data. Run the **insert whatever dag we end up using** file in the dags folder. Our own data has been provided as an example, and you can replace it with yours/your friends to get recommendations on your own listening history. This will insert the normalized data tables into Snowflake. 

### Step 3: 
Get new releases. Run the new_releases.py **fix name if thats not right** file. This is scheduled to run weekly, and will update the songs table following each run. 

### Step 4:
After all tables have been created/updated within Snowflake, we run of files to get our final recommendations. These files will be found in the snowflake_worksheets folder. 
1. Run create_model_features.sql as a Snowflake worksheet. This feature engineers the features we need for our model, and outputs them in the recommendation_features table. 
2. Run recommendation_model.py as a Snowflake worksheet. You can change the different parameters you want your playlist to take into account in the main function. This will output a table containing 30 songs and their details in the final_recommendations table. At this point, you now have the output of our model.

### Step 5 (Optional):
If you would like a real Spotify playlist created from the final_recommendations table, we have code for it! General disclaimer however, it may be difficult to run this code because there have been issues on Spotify's end in allowing newly created API keys to run this portion. 
1. Run snowflake_worksheets/get_json.py. This will output a JSON formatted list of Spotify song IDs. Copy this.
2. Navigate to the create_playlist folder. In queue.json, paste the output from step 1.
3. Create a .env file. The contents will look like this (replace with your own credentials):
   ```bash
   SPOTIPY_CLIENT_ID=your_spotify_client_id
   SPOTIPY_CLIENT_SECRET=your_spotify_client_secret
   SPOTIPY_REDIRECT_URI=http://127.0.0.1:8889/callback
   ```
5. In your terminal, run `python playlist.py --init --name "Example playlist"`
Now you have a playlist in your own account!
