import os
import json
import random
import time
import sys
import logging
import shutil
from pathlib import Path

import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotdl import Spotdl

# --- 1. LOGGING & SAFETY GATE ---
log_format = '%(asctime)s | %(levelname)s | %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format, datefmt='%H:%M:%S', stream=sys.stdout)
logging.getLogger("spotipy").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("spotdl").setLevel(logging.INFO) 

if not shutil.which("ffmpeg"):
    print("\n" + "!"*60)
    print("❌ CRITICAL ERROR: FFmpeg is missing from your system PATH.")
    print("SpotDL needs it to tag the audio files. Run this in your terminal:")
    print("sudo apt update && sudo apt install ffmpeg")
    print("!"*60 + "\n")
    sys.exit(1)

env_base = os.environ.get("BASE_PATH")
if not env_base or env_base == ".":
    print("\n" + "!"*60)
    print("❌ PATH ERROR: $BASE_PATH is not set.")
    print("FIX: export BASE_PATH='/mnt/e/Music'")
    print("!"*60 + "\n")
    sys.exit(1)

BASE_PATH = Path(env_base).resolve()
MAP_FILE = "snapshot_map.json"
NUM_THREADS = 1

class SpotifyClient:
    def __init__(self):
        self.username = os.environ.get("USERNAME")
        self.auth_manager = SpotifyOAuth(scope="playlist-read-private", open_browser=False)
        self.sp = spotipy.Spotify(auth_manager=self.auth_manager, requests_timeout=60)
        self.target_snapshot_map = {}

    def check_initial_rate_limit(self):
        try:
            self.sp.me()
            return True
        except Exception:
            return False

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

class SpotdlSync:
    def __init__(self, audio_format="m4a", bitrate="disable"):
        self.spotify = SpotifyClient()
        self.client_id = os.environ.get("SPOTIPY_CLIENT_ID")
        self.client_secret = os.environ.get("SPOTIPY_CLIENT_SECRET")
        self.audio_format = audio_format
        self.bitrate = bitrate

    def visual_countdown(self, seconds, reason="Cooldown"):
        for i in range(int(seconds), 0, -1):
            sys.stdout.write(f"\r⏳ {reason}: {i}s remaining...   ")
            sys.stdout.flush()
            time.sleep(1)
        sys.stdout.write("\r" + " " * 85 + "\r")

    def run(self):
        temp_path = Path.home() / ".spotdl" / "temp"
        if temp_path.exists():
            shutil.rmtree(temp_path, ignore_errors=True)
        temp_path.mkdir(parents=True, exist_ok=True)

        print("\n" + "="*60)
        print(f"📂 DESTINATION: {BASE_PATH}")
        print(f"🎵 FORMAT:      {self.audio_format.upper()} ({self.bitrate if self.bitrate != 'disable' else 'Native'})")
        print("⚙️  THREADS:     1 (Strict Anti-Rate Limit Mode)")
        print("="*60)

        if not self.spotify.check_initial_rate_limit():
            logging.critical("⛔ 24-hour Spotify ban active.")
            return

        logging.info("Scanning Spotify library...")
        self.spotify.target_snapshot_map = self.spotify.get_all_playlists()
        
        local_map = {}
        if Path(MAP_FILE).exists():
            with open(MAP_FILE, 'r') as f: 
                local_map = json.load(f)

        all_names = sorted(self.spotify.target_snapshot_map.keys())
        
        print("\n" + "="*60)
        print("      SPOTIFY PLAYLIST MENU")
        print("="*60)
        print("  0. [SYNC ALL PENDING]")
        print("  F. [FORCE UPDATE ALL]")
        for i, name in enumerate(all_names, 1):
            target = self.spotify.target_snapshot_map[name]
            is_up = name in local_map and local_map[name]['snapshot_id'] == target['snapshot_id']
            print(f"  {i:2}. {'[UP TO DATE]' if is_up else '[PENDING]'} {name[:25]}")
        
        choice = input(f"\nSelect 0, F, or 1-{len(all_names)}: ").strip().lower()

        queue = []
        if choice == '0':
            queue = [n for n in all_names if n not in local_map or local_map[n]['snapshot_id'] != self.spotify.target_snapshot_map[n]['snapshot_id']]
        elif choice == 'f':
            queue = all_names
        elif choice.isdigit() and 1 <= int(choice) <= len(all_names):
            name = all_names[int(choice)-1]
            queue = [name]

        original_cwd = os.getcwd()

        for name in queue:
            item = self.spotify.target_snapshot_map[name]
            playlist_dir = BASE_PATH / name
            playlist_dir.mkdir(parents=True, exist_ok=True)
            
            if len(queue) > 1:
                self.visual_countdown(random.uniform(8, 15), f"Staggering Playlist: {name}")
            
            logging.info(f"\n🚀 Syncing: {name}")
            logging.info(f"📂 Path: {playlist_dir}")

            try:
                os.chdir(playlist_dir)

                spotdl_instance = Spotdl(
                    client_id=self.client_id,
                    client_secret=self.client_secret,
                    downloader_settings={
                        "threads": NUM_THREADS,
                        "format": self.audio_format,
                        "bitrate": self.bitrate, 
                        "output": "{artists} - {title}"
                    }
                )
                
                songs = spotdl_instance.search([item['url']])
                spotdl_instance.download_songs(songs)
                
                downloaded_files = list(Path(".").glob(f"*.{self.audio_format}"))
                if len(downloaded_files) > 0:
                    local_map[name] = item
                    with open(Path(original_cwd) / MAP_FILE, 'w') as f: 
                        json.dump(local_map, f, indent=4, sort_keys=True)
                    logging.info(f"✅ SUCCESS: {name} ({len(downloaded_files)} files total)\n")
                else:
                    logging.warning(f"⚠️ {name} completed but 0 files were saved. Snapshot not updated.\n")

            except Exception as e:
                if "429" in str(e) or "Too Many Requests" in str(e):
                    logging.error("⛔ YOUTUBE RATE LIMIT BAN. Stopping script to prevent deeper ban.")
                    sys.exit(1)
                logging.error(f"❌ Error syncing {name}: {e}")
            finally:
                os.chdir(original_cwd)

if __name__ == "__main__":
    print("\n--- FORMAT SELECTION ---")
    print("1. M4A (Native YouTube AAC - Zero Bloat)")
    print("2. MP3 (128kbps - Size-Optimized MP3)")
    fmt_input = input("Choice (1-2) [Default 1]: ").strip()
    
    selected_fmt = "mp3" if fmt_input == "2" else "m4a"
    selected_bitrate = "128k" if selected_fmt == "mp3" else "disable"

    SpotdlSync(audio_format=selected_fmt, bitrate=selected_bitrate).run()
