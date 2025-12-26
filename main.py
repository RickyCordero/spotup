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
log_format = '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    datefmt='%H:%M:%S',
    stream=sys.stdout
)

# Silence internal noise to keep the terminal clean
logging.getLogger("spotipy").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("spotdl").setLevel(logging.CRITICAL)

# --- 2. SETTINGS ---
BASE_PATH = os.environ.get("BASE_PATH", ".")
NUM_THREADS = 1         # Stick to 1 for now to recover from the 83k ban
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
            backoff_factor=2.0
        )
        self.target_snapshot_map = {}
        self._cached_map = None

    def check_initial_rate_limit(self):
        """Verifies if the 23-hour ban is still active."""
        try:
            self.sp.me()
            return True
        except Exception as e:
            if "429" in str(e):
                return False
            return True # Other errors will be caught later

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
        sys.stdout.write("\r" + " " * 85 + "\r") 

    def process_playlist(self, item):
        name, url, count = item["name"], item["url"], item["count"]
        path = Path(BASE_PATH) / name
        path.mkdir(parents=True, exist_ok=True)

        for attempt in range(MAX_RETRIES):
            try:
                logging.info(f"🚀 Processing: {name} ({count} tracks)")
                songs = self.spotdl.search([url])
                self.spotdl.downloader.settings["output"] = str(path)
                self.spotdl.download_songs(songs)
                
                entry = self.spotify_client.target_snapshot_map.get(name)
                self.spotify_client.atomic_save(name, entry)
                logging.info(f"✅ SUCCESS: {name}")
                return True

            except Exception as e:
                err_msg = str(e)
                if "429" in err_msg:
                    if "83000" in err_msg or "50000" in err_msg:
                        logging.error(f"⛔ HARD BAN DETECTED ({err_msg}). Please wait 24 hours.")
                        sys.exit(1)
                    
                    wait_time = 180 * (attempt + 1)
                    logging.warning(f"⚠️ Rate Limit on {name}. Backing off.")
                    self.visual_countdown(wait_time, "API Backoff")
                else:
                    logging.error(f"❌ Error on {name}: {e}")
                    return False

    def download(self):
        if not os.path.exists(BASE_PATH):
            logging.error(f"WSL Mount Error: {BASE_PATH} is inaccessible.")
            return

        if not self.spotify_client.check_initial_rate_limit():
            logging.critical("⛔ ACCOUNT LOCKED: The 24-hour Spotify ban is still active.")
            return

        logging.info("Fetching library data...")
        self.spotify_client.target_snapshot_map = self.spotify_client.get_all_playlists()
        local_map = self.spotify_client.load_map()
        
        queue = [
            {"name": n, "url": obj["url"], "count": obj["track_count"]} 
            for n, obj in self.spotify_client.target_snapshot_map.items()
            if n not in local_map or local_map[n]["snapshot_id"] != obj["snapshot_id"]
        ]

        if not queue:
            logging.info("Everything is already up to date!")
            return

        # --- INTERACTIVE MENU ---
        print("\n" + "="*45)
        print("      SPOTIFY PLAYLIST DOWNLOADER")
        print("="*45)
        print("  0. [PROCESS ALL PENDING PLAYLISTS]")
        for i, item in enumerate(queue, 1):
            print(f"  {i:2}. {item['name']:30} ({item['count']:4} tracks)")
        print("="*45)
        
        try:
            choice = int(input(f"\nSelect an option (0-{len(queue)}): "))
        except ValueError:
            print("Invalid input. Exiting.")
            return

        if choice == 0:
            random.shuffle(queue)
            logging.info(f"Starting batch process for {len(queue)} playlists...")
            for item in queue:
                dynamic_delay = 10 + (item['count'] // 40)
                self.visual_countdown(random.uniform(5, dynamic_delay), f"Staggering: {item['name']}")
                self.process_playlist(item)
                if item['count'] > 300: self.visual_countdown(45, "Post-Mega Cooling")
        elif 1 <= choice <= len(queue):
            self.process_playlist(queue[choice - 1])
        else:
            print("Choice out of range.")

if __name__ == "__main__":
    SpotdlClient().download()
