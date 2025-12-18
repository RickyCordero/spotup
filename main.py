import os
import json
import random
import time
import tempfile
import sys
import logging
from pathlib import Path

import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotdl import Spotdl

# --- 1. ELEGANT LOGGING CONFIGURATION ---
# This captures all logs (including libraries) and forces them into our format
log_format = '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    datefmt='%H:%M:%S',
    stream=sys.stdout
)

# --- 2. SETTINGS ---
BASE_PATH = os.environ.get("BASE_PATH", ".")
NUM_THREADS = 2         
MAX_RETRIES = 5
MAP_FILE = "snapshot_map.json"

class SpotifyClient():
    def __init__(self):
        self.username = os.environ.get("USERNAME")
        self.auth_manager = SpotifyOAuth(scope="playlist-read-private", open_browser=False)
        self.sp = spotipy.Spotify(
            auth_manager=self.auth_manager,
            requests_timeout=60,
            retries=15,
            backoff_factor=1.5
        )
        self.target_snapshot_map = {}
        self._cached_map = None

    def check_initial_rate_limit(self):
        """Checks if the account is currently banned/limited before starting."""
        try:
            self.sp.me()
            return True
        except Exception as e:
            if "429" in str(e):
                logging.error("⛔ CRITICAL: You are currently rate-limited by Spotify (likely the 14-hour ban).")
                return False
            logging.error(f"⚠️ Connection Error: {e}")
            return False

    def load_map(self):
        if self._cached_map is not None: return self._cached_map
        if not Path(MAP_FILE).exists(): return {}
        try:
            with open(MAP_FILE, "r") as f:
                self._cached_map = json.load(f)
                return self._cached_map
        except: return {}

    def atomic_save(self, name, entry):
        data = self.load_map()
        data[name] = entry
        self._cached_map = data
        fd, temp = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(MAP_FILE)))
        try:
            with os.fdopen(fd, 'w') as tmp:
                json.dump(data, tmp, indent=4, sort_keys=True)
            os.replace(temp, MAP_FILE)
        except:
            if os.path.exists(temp): os.remove(temp)

    def get_all_playlists(self):
        full_map = {}
        results = self.sp.current_user_playlists(limit=50)
        while results:
            for item in results['items']:
                if item and item['owner']['id'] == self.username:
                    full_map[item['name']] = {
                        "url": item['external_urls']['spotify'],
                        "snapshot_id": item['snapshot_id'],
                        "track_count": item['tracks']['total']
                    }
            results = self.sp.next(results) if results['next'] else None
        return full_map

class SpotdlClient():
    def __init__(self):
        self.spotify_client = SpotifyClient()
        self.spotdl = Spotdl(
            client_id=os.environ.get("SPOTIPY_CLIENT_ID"),
            client_secret=os.environ.get("SPOTIPY_CLIENT_SECRET"),
            downloader_settings={
                "threads": NUM_THREADS, 
                "silent": False,      
                "fetch_albums": False 
            },
        )

    def visual_countdown(self, seconds, reason="Cooldown"):
        for i in range(int(seconds), 0, -1):
            sys.stdout.write(f"\r⏳ {reason}: {i}s remaining...   ")
            sys.stdout.flush()
            time.sleep(1)
        sys.stdout.write("\r" + " " * 80 + "\r") 

    def download(self):
        if not os.path.exists(BASE_PATH):
            logging.error(f"WSL Mount Error: {BASE_PATH} is inaccessible.")
            return

        # --- PRE-FLIGHT CHECK ---
        if not self.spotify_client.check_initial_rate_limit():
            return

        logging.info("Starting Spotify Sync (Mega-Playlist Optimized)")
        self.spotify_client.target_snapshot_map = self.spotify_client.get_all_playlists()
        local_map = self.spotify_client.load_map()
        
        queue = [
            {"name": n, "url": obj["url"], "count": obj["track_count"]} 
            for n, obj in self.spotify_client.target_snapshot_map.items()
            if n not in local_map or local_map[n]["snapshot_id"] != obj["snapshot_id"]
        ]
        
        random.shuffle(queue)
        logging.info(f"Queue: {len(queue)} playlists pending.")

        for item in queue:
            name, url, count = item["name"], item["url"], item["count"]
            path = Path(BASE_PATH) / name
            path.mkdir(parents=True, exist_ok=True)

            dynamic_delay = 10 + (count // 40)
            self.visual_countdown(random.uniform(5, dynamic_delay), f"Pre-sync: {name}")

            for attempt in range(MAX_RETRIES):
                try:
                    logging.info(f"SEARCHING: {name} ({count} tracks)")
                    songs = self.spotdl.search([url])
                    
                    self.spotdl.downloader.settings["output"] = str(path)
                    self.spotdl.download_songs(songs)
                    
                    entry = self.spotify_client.target_snapshot_map.get(name)
                    self.spotify_client.atomic_save(name, entry)
                    logging.info(f"SUCCESS: {name} saved to disk.")
                    
                    if count > 400:
                        self.visual_countdown(60, "Mega-Playlist Buffer Recovery")
                    break 

                except Exception as e:
                    if "429" in str(e):
                        wait_time = 180 * (attempt + 1) # 3-minute backoff for 429s
                        logging.warning(f"RATE LIMIT: {name}. Applying heavy backoff.")
                        self.visual_countdown(wait_time, "API Cooldown")
                    else:
                        logging.error(f"SKIPPED: {name} due to {e}")
                        break

if __name__ == "__main__":
    SpotdlClient().download()
