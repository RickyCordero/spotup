import os
import json
import random
import time
import sys
import logging
import shutil
import subprocess
import re
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
MAP_FILE = Path(__file__).resolve().parent / "snapshot_map.json"
NUM_THREADS = 1

def sanitize_filename(name):
    """
    Sanitizes playlist names to be safe for directory names on Windows/Linux.
    """
    for char in ['\\', '/', ':', '*', '?', '"', '<', '>', '|']:
        name = name.replace(char, '_')
    return name

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
        if MAP_FILE.exists():
            with open(MAP_FILE, 'r') as f: 
                local_map = json.load(f)

        all_names = sorted(self.spotify.target_snapshot_map.keys())
        
        print("\n" + "="*60)
        print("      SPOTIFY PLAYLIST MENU")
        print("="*60)
        print("  0. [SYNC ALL PENDING]")
        print("  F. [FORCE UPDATE ALL]")
        print("  D. [DEDUPLICATE MP3s]")
        for i, name in enumerate(all_names, 1):
            target = self.spotify.target_snapshot_map[name]
            is_up = name in local_map and local_map[name]['snapshot_id'] == target['snapshot_id']
            print(f"  {i:2}. {'[UP TO DATE]' if is_up else '[PENDING]'} {name[:25]}")
        
        choice = input(f"\nSelect 0, F, D, or 1-{len(all_names)}: ").strip().lower()

        queue = []
        if choice == '0':
            queue = [n for n in all_names if n not in local_map or local_map[n]['snapshot_id'] != self.spotify.target_snapshot_map[n]['snapshot_id']]
        elif choice == 'f':
            queue = all_names
        elif choice == 'd':
            deduplicate_tracks()
            return
        elif choice.isdigit() and 1 <= int(choice) <= len(all_names):
            name = all_names[int(choice)-1]
            queue = [name]

        original_cwd = os.getcwd()

        for name in queue:
            item = self.spotify.target_snapshot_map[name]
            sanitized_name = sanitize_filename(name)
            playlist_dir = BASE_PATH / sanitized_name
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
                    with open(MAP_FILE, 'w') as f: 
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

        if queue:
            print()
            for name in queue:
                deduplicate_tracks(BASE_PATH / sanitize_filename(name))

def get_audio_metadata(file_path):
    """
    Retrieves duration, bitrate, title, and artist of an audio file using ffprobe.
    Returns a dictionary or None on failure.
    """
    metadata = {
        "duration": 0.0,
        "bitrate": 0,
        "title": "",
        "artist": ""
    }
    try:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration,bit_rate:format_tags=title,artist",
            "-of", "default=noprint_wrappers=1",
            str(file_path)
        ]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        for line in result.stdout.strip().splitlines():
            if "=" in line:
                key, val = line.split("=", 1)
                if key == "duration":
                    metadata["duration"] = float(val)
                elif key == "bit_rate":
                    metadata["bitrate"] = int(val) if val.isdigit() else 0
                elif key == "TAG:title":
                    metadata["title"] = val.strip()
                elif key == "TAG:artist":
                    metadata["artist"] = val.strip()
        return metadata
    except Exception as e:
        logging.warning(f"⚠️ Failed to read metadata for {file_path.name}: {e}")
    return None

def normalize_string(s):
    if not s:
        return ""
    s = s.lower()
    # Remove text inside parentheses or brackets like "(feat. ...)" or "[Remastered]"
    s = re.sub(r'[\(\[][^)]*[\)\]]', '', s)
    # Remove all non-alphanumeric characters
    s = re.sub(r'[^a-z0-9]', '', s)
    return s

def is_duplicate_match(f1, f2):
    meta1, meta2 = f1["meta"], f2["meta"]
    
    # 1. First enforce that durations must match within 2.0 seconds
    duration_diff = abs(meta1["duration"] - meta2["duration"])
    if duration_diff > 2.0:
        return False
        
    # 2. Compare normalized Title and Artist tags
    norm_title1 = normalize_string(meta1["title"])
    norm_title2 = normalize_string(meta2["title"])
    norm_artist1 = normalize_string(meta1["artist"])
    norm_artist2 = normalize_string(meta2["artist"])
    
    if norm_title1 and norm_title2 and norm_artist1 and norm_artist2:
        if norm_title1 == norm_title2 and norm_artist1 == norm_artist2:
            return True
            
    # 3. Fallback: Compare normalized filename stems
    stem1 = normalize_string(f1["path"].stem)
    stem2 = normalize_string(f2["path"].stem)
    if stem1 == stem2:
        return True
        
    return False

def resolve_duplicate(f1, f2):
    """
    Decides which file to keep and which to delete.
    Returns (keep_file, delete_file).
    """
    path1, path2 = f1["path"], f2["path"]
    ext1, ext2 = path1.suffix.lower(), path2.suffix.lower()
    
    # Rule 1: Always prefer M4A over MP3
    if ext1 == ".m4a" and ext2 == ".mp3":
        return f1, f2
    if ext1 == ".mp3" and ext2 == ".m4a":
        return f2, f1
        
    # Rule 2: If same extension, keep higher bitrate version
    br1, br2 = f1["meta"]["bitrate"], f2["meta"]["bitrate"]
    if br1 > br2:
        return f1, f2
    if br2 > br1:
        return f2, f1
        
    # Rule 3: If same bitrate, keep the shorter (cleaner) filename
    if len(path1.name) <= len(path2.name):
        return f1, f2
    else:
        return f2, f1

def deduplicate_tracks(target_path=None):
    scan_path = target_path if target_path else BASE_PATH
    logging.info(f"Checking for duplicates under {scan_path}...")
    if not scan_path.exists():
        logging.warning(f"Scan path {scan_path} does not exist. Nothing to deduplicate.")
        return

    # Find all audio files
    all_files = []
    for ext in ["*.m4a", "*.mp3"]:
        all_files.extend(list(scan_path.rglob(ext)))

    # Group files by normalized filename stem
    groups = {}
    for path in all_files:
        if not path.exists():
            continue
        stem_norm = normalize_string(path.stem)
        if stem_norm not in groups:
            groups[stem_norm] = []
        groups[stem_norm].append(path)

    # Filter groups to only keep potential duplicate groups (size >= 2)
    duplicate_groups = {k: v for k, v in groups.items() if len(v) >= 2}

    if not duplicate_groups:
        logging.info("✨ Deduplication finished. No duplicate files found.")
        return

    logging.info(f"Analyzing {len(duplicate_groups)} potential duplicate group(s)...")

    # Gather metadata ONLY for potential duplicates
    audio_files = []
    for stem_norm, paths in duplicate_groups.items():
        for path in paths:
            if not path.exists():
                continue
            meta = get_audio_metadata(path)
            if meta:
                audio_files.append({"path": path, "meta": meta})

    # Sort files by duration to optimize comparison
    audio_files.sort(key=lambda x: x["meta"]["duration"])

    dedup_count = 0
    deleted_paths = set()

    for i in range(len(audio_files)):
        f1 = audio_files[i]
        if f1["path"] in deleted_paths or not f1["path"].exists():
            continue
            
        for j in range(i + 1, len(audio_files)):
            f2 = audio_files[j]
            if f2["path"] in deleted_paths or not f2["path"].exists():
                continue
                
            # If duration difference is > 2 seconds, break the inner loop (since list is sorted)
            if f2["meta"]["duration"] - f1["meta"]["duration"] > 2.0:
                break
                
            # Check if they match
            if is_duplicate_match(f1, f2):
                keep, delete = resolve_duplicate(f1, f2)
                
                br_keep = f"{keep['meta']['bitrate'] // 1000}kbps" if keep['meta']['bitrate'] else "unknown"
                br_del = f"{delete['meta']['bitrate'] // 1000}kbps" if delete['meta']['bitrate'] else "unknown"
                
                logging.info(
                    f"🗑️ Duplicate found: Removing '{delete['path'].name}' ({br_del}) "
                    f"in favor of '{keep['path'].name}' ({br_keep})"
                )
                try:
                    delete["path"].unlink()
                    deleted_paths.add(delete["path"])
                    dedup_count += 1
                except Exception as e:
                    logging.error(f"❌ Failed to delete {delete['path']}: {e}")
                    
    logging.info(f"✨ Deduplication finished. Removed {dedup_count} duplicate file(s).")

if __name__ == "__main__":
    print("\n--- FORMAT SELECTION ---")
    print("1. M4A (Native YouTube AAC - Zero Bloat)")
    print("2. MP3 (128kbps - Size-Optimized MP3)")
    print("D. Deduplicate Tracks (Remove MP3 if exact M4A exists)")
    fmt_input = input("Choice (1-2, D) [Default 1]: ").strip().lower()
    
    if fmt_input == 'd':
        deduplicate_tracks()
        sys.exit(0)
        
    selected_fmt = "mp3" if fmt_input == "2" else "m4a"
    selected_bitrate = "128k" if selected_fmt == "mp3" else "disable"

    SpotdlSync(audio_format=selected_fmt, bitrate=selected_bitrate).run()
