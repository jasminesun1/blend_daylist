import argparse
import json
import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv

load_dotenv()

SPOTIPY_CLIENT_ID = os.getenv('SPOTIPY_CLIENT_ID')
SPOTIPY_CLIENT_SECRET = os.getenv('SPOTIPY_CLIENT_SECRET')
SPOTIPY_REDIRECT_URI = os.getenv('SPOTIPY_REDIRECT_URI')

# api creds
SPOTIPY_CLIENT_ID = SPOTIPY_CLIENT_ID
SPOTIPY_CLIENT_SECRET = SPOTIPY_CLIENT_SECRET
SPOTIPY_REDIRECT_URI = 'http://127.0.0.1:8889/callback'
SCOPE = 'playlist-modify-public playlist-modify-private playlist-read-private'

# initialize spotify client
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=SPOTIPY_CLIENT_ID,
    client_secret=SPOTIPY_CLIENT_SECRET,
    redirect_uri=SPOTIPY_REDIRECT_URI,
    scope=SCOPE,
    open_browser=False,
    cache_path=".cache"
))

# files
QUEUE_FILE = 'queue.json'
ADDED_FILE = '.added_tracks.json'

def load_queue():
    with open(QUEUE_FILE) as f:
        return json.load(f)

def save_added_tracks(track_ids):
    with open(ADDED_FILE, 'w') as f:
        json.dump(track_ids, f)

def load_added_tracks():
    if not os.path.exists(ADDED_FILE):
        return set()
    with open(ADDED_FILE) as f:
        return set(json.load(f))

def create_playlist(name):
    user_id = sp.current_user()["id"]
    playlist = sp.user_playlist_create(user=user_id, name=name, public=False)
    return playlist['id']

def get_playlist_tracks(playlist_id):
    tracks = []
    results = sp.playlist_items(playlist_id)
    while results:
        tracks += [item['track']['id'] for item in results['items']]
        if results['next']:
            results = sp.next(results)
        else:
            break
    return set(tracks)

def add_songs_to_playlist(playlist_id, track_ids):
    uris = [f"spotify:track:{tid}" for tid in track_ids]
    sp.playlist_add_items(playlist_id, uris)

PLAYLIST_FILE = '.playlist_id'

def save_playlist_id(playlist_id):
    with open(PLAYLIST_FILE, 'w') as f:
        f.write(playlist_id)

def load_playlist_id():
    if not os.path.exists(PLAYLIST_FILE):
        return None
    with open(PLAYLIST_FILE) as f:
        return f.read().strip()

def main(args):
    queue = load_queue()
    added = load_added_tracks()

    if args.init:
        print("Initializing new playlist...")
        playlist_id = create_playlist(args.name)
        save_playlist_id(playlist_id)
        add_songs_to_playlist(playlist_id, queue)
        save_added_tracks(queue)
        print(f"Created playlist '{args.name}' with {len(queue)} songs.")

    elif args.update:
        playlist_id = load_playlist_id()
        if not playlist_id:
            print("No saved playlist ID found. Run with --init first.")
            return

        new_tracks = [tid for tid in queue if tid not in added]

        if new_tracks:
            add_songs_to_playlist(playlist_id, new_tracks)
            added.update(new_tracks)
            save_added_tracks(list(added))
            print(f"Added {len(new_tracks)} new songs to the playlist.")
        else:
            print("No new tracks to add.")

    else:
        print("Please use either --init or --update flag.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage a Spotify playlist via a queue.")
    parser.add_argument('--init', action='store_true', help='Initialize a new playlist from the queue')
    parser.add_argument('--update', action='store_true', help='Update existing playlist with new queue songs')
    parser.add_argument('--playlist', type=str, help='Existing playlist ID to update')
    parser.add_argument('--name', type=str, default='AI DJ Playlist', help='Name for new playlist')
    main(parser.parse_args())
