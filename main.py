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
from spotipy.exceptions import SpotifyOauthError, SpotifyException
from spotdl import Spotdl
from tinytag import TinyTag
from dotenv import load_dotenv

load_dotenv()


# --- 1. LOGGING & SAFETY GATE ---
log_format = '%(asctime)s | %(levelname)s | %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format, datefmt='%H:%M:%S', stream=sys.stdout)
logging.getLogger("spotipy").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("spotdl").setLevel(logging.INFO) 
# Check local spotdl configuration directories for ffmpeg first
local_ffmpeg_paths = [
    Path.home() / ".config" / "spotdl",
    Path.home() / ".spotdl",
]
for p in local_ffmpeg_paths:
    ffmpeg_bin = p / "ffmpeg"
    if ffmpeg_bin.is_file() and os.access(ffmpeg_bin, os.X_OK):
        os.environ["PATH"] = str(p) + os.path.pathsep + os.environ.get("PATH", "")
        break

if not shutil.which("ffmpeg"):
    print("\n" + "!"*60)
    print("❌ CRITICAL ERROR: FFmpeg is missing from your system PATH.")
    print("SpotDL needs it to tag the audio files. Run this in your terminal:")
    print("sudo apt update && sudo apt install ffmpeg")
    print("or run:")
    print("uv run spotdl --download-ffmpeg")
    print("!"*60 + "\n")
    sys.exit(1)

required_envs = ["BASE_PATH", "SPOTIPY_CLIENT_ID", "SPOTIPY_CLIENT_SECRET", "USERNAME"]
missing_envs = [var for var in required_envs if not os.environ.get(var) or os.environ.get(var) == "."]
if missing_envs:
    print("\n" + "!"*60)
    print(f"❌ CONFIG ERROR: Missing environment variable(s): {', '.join(missing_envs)}")
    print("FIX: Ensure these variables are set in your environment or .env file.")
    print("!"*60 + "\n")
    sys.exit(1)

BASE_PATH = Path(os.environ.get("BASE_PATH")).resolve()
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
        except SpotifyOauthError as e:
            logging.warning("🔑 Spotify authorization token in '.cache' is invalid or revoked.")
            cache_path = Path(".cache")
            if cache_path.exists():
                logging.info("Removing stale .cache file for re-authentication...")
                cache_path.unlink(missing_ok=True)
            self.auth_manager = SpotifyOAuth(scope="playlist-read-private", open_browser=False)
            self.sp = spotipy.Spotify(auth_manager=self.auth_manager, requests_timeout=60)
            try:
                self.sp.me()
                return True
            except Exception as retry_e:
                logging.error(f"❌ Spotify authentication failed: {retry_e}")
                return False
        except SpotifyException as e:
            if getattr(e, "http_status", None) == 429:
                logging.critical("⛔ Spotify rate limit (429) active.")
                return False
            logging.error(f"❌ Spotify API error: {e}")
            return False
        except Exception as e:
            logging.error(f"❌ Spotify connection error: {e}")
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
        temp_path.mkdir(parents=True, exist_ok=True)

        print("\n" + "="*60)
        print(f"📂 DESTINATION: {BASE_PATH}")
        print(f"🎵 FORMAT:      {self.audio_format.upper()} ({self.bitrate if self.bitrate != 'disable' else 'Native'})")
        print("⚙️  THREADS:     1 (Strict Anti-Rate Limit Mode)")
        print("="*60)

        if not self.spotify.check_initial_rate_limit():
            logging.critical("⛔ Spotify pre-flight check failed. Sync aborted.")
            return

        logging.info("Scanning Spotify library...")
        self.spotify.target_snapshot_map = self.spotify.get_all_playlists()
        
        local_map = {}
        if MAP_FILE.exists():
            with open(MAP_FILE, 'r') as f: 
                local_map = json.load(f)

        all_names = sorted(self.spotify.target_snapshot_map.keys())
        pending_count = sum(
            1 for n in all_names 
            if n not in local_map or local_map[n]['snapshot_id'] != self.spotify.target_snapshot_map[n]['snapshot_id']
        )
        
        print("\n" + "="*60)
        print("      SPOTIFY PLAYLIST MENU")
        print("="*60)
        print(f"  0. [SYNC ALL PENDING ({pending_count} pending)]")
        print(f"  F. [FORCE UPDATE ALL ({len(all_names)} total)]")
        print("  D. [DEDUPLICATE TRACKS]")
        for i, name in enumerate(all_names, 1):
            target = self.spotify.target_snapshot_map[name]
            is_up = name in local_map and local_map[name]['snapshot_id'] == target['snapshot_id']
            status = "[UP TO DATE]" if is_up else "[PENDING]   "
            track_count = target.get('track_count', '?')
            print(f"  {i:2}. {status} {name[:30]:<30} ({track_count} tracks)")
        
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
                    temp_map_file = MAP_FILE.with_suffix(".tmp")
                    with open(temp_map_file, 'w') as f: 
                        json.dump(local_map, f, indent=4, sort_keys=True)
                    temp_map_file.replace(MAP_FILE)
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
    Retrieves duration, bitrate, title, and artist of an audio file using TinyTag.
    Returns a dictionary or None on failure.
    """
    metadata = {
        "duration": 0.0,
        "bitrate": 0,
        "title": "",
        "artist": ""
    }
    try:
        tag = TinyTag.get(str(file_path))
        metadata["duration"] = float(tag.duration) if tag.duration else 0.0
        metadata["bitrate"] = int(tag.bitrate * 1000) if tag.bitrate else 0
        metadata["title"] = tag.title.strip() if tag.title else ""
        metadata["artist"] = tag.artist.strip() if tag.artist else ""
        return metadata
    except Exception as e:
        logging.warning(f"⚠️ Failed to read metadata for {file_path.name}: {e}")
    return metadata

def normalize_string(s):
    if not s:
        return ""
    s = s.lower()
    # Remove text inside parentheses or brackets like "(feat. ...)" or "[Remastered]"
    s = re.sub(r'[\(\[\{][^\]\}\)]*[\)\]\}]', '', s)
    # Remove all non-alphanumeric characters
    s = re.sub(r'[^a-z0-9]', '', s)
    return s

def clean_title(title):
    if not title:
        return ""
    s = title.lower()
    # Strip featuring clauses (whether in parentheses or unparenthesized)
    s = re.sub(r'[\(\[\{]?\b(feat|ft|featuring)\.?\b.*$', '', s)
    # Strip remaining parenthetical or bracketed text
    s = re.sub(r'[\(\[\{][^\]\}\)]*[\)\]\}]', '', s)
    # Strip non-alphanumeric characters
    s = re.sub(r'[^a-z0-9]', '', s)
    return s

def clean_artist(artist):
    if not artist:
        return set()
    s = artist.lower()
    s = re.sub(r'[\(\[\{]?\b(feat|ft|featuring)\.?\b.*$', '', s)
    # Split by delimiters like commas, &, and, x
    parts = re.split(r'[,&]|\bx\b|\band\b', s)
    artists = set()
    for p in parts:
        cleaned = re.sub(r'[^a-z0-9]', '', p)
        if cleaned:
            artists.add(cleaned)
    return artists

def parse_audio_file(path):
    meta = get_audio_metadata(path)
    stem = path.stem
    if ' - ' in stem:
        fn_artist, fn_title = stem.split(' - ', 1)
    else:
        fn_artist, fn_title = '', stem
        
    title = meta["title"] or fn_title
    artist = meta["artist"] or fn_artist
    
    c_title = clean_title(title)
    if not c_title:
        c_title = clean_title(fn_title)
    if not c_title:
        c_title = normalize_string(stem)
        
    artists = clean_artist(artist) | clean_artist(fn_artist) | clean_artist(meta["artist"])
    
    return {
        "path": path,
        "meta": meta,
        "c_title": c_title,
        "artists": artists
    }

def is_duplicate_match(f1, f2):
    """
    Checks if two parsed audio file dicts represent duplicate tracks based on duration, artist overlap, and title.
    """
    dur1 = f1["meta"]["duration"]
    dur2 = f2["meta"]["duration"]
    if dur1 > 0 and dur2 > 0 and abs(dur1 - dur2) > 4.0:
        return False
        
    # Require artist overlap if artist metadata is available on both files
    if f1["artists"] and f2["artists"]:
        if not (f1["artists"] & f2["artists"]):
            return False
    else:
        # If artist metadata is missing on either file, require exact normalized stem match
        stem1 = normalize_string(f1["path"].stem)
        stem2 = normalize_string(f2["path"].stem)
        if stem1 != stem2:
            return False
        
    return True

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
    if not target_path:
        # Run deduplication on each individual playlist directory under BASE_PATH
        if not BASE_PATH.exists():
            logging.warning(f"Scan path {BASE_PATH} does not exist. Nothing to deduplicate.")
            return
        for item in BASE_PATH.iterdir():
            if item.is_dir():
                deduplicate_tracks(item)
        return

    scan_path = target_path
    logging.info(f"Checking for duplicates under {scan_path}...")
    if not scan_path.exists():
        logging.warning(f"Scan path {scan_path} does not exist. Nothing to deduplicate.")
        return

    # Find all audio files
    all_files = []
    for ext in ["*.m4a", "*.mp3"]:
        all_files.extend(list(scan_path.rglob(ext)))

    if not all_files:
        logging.info("✨ Deduplication finished. No audio files found.")
        return

    # Parse metadata for all audio files
    parsed_files = [parse_audio_file(p) for p in all_files if p.exists()]

    # Group files by cleaned title
    groups = {}
    for f in parsed_files:
        t = f["c_title"]
        if t not in groups:
            groups[t] = []
        groups[t].append(f)

    duplicate_groups = {k: v for k, v in groups.items() if len(v) >= 2}

    if not duplicate_groups:
        logging.info("✨ Deduplication finished. No duplicate files found.")
        return

    logging.info(f"Analyzing {len(duplicate_groups)} potential duplicate group(s)...")

    dedup_count = 0
    deleted_paths = set()

    for t, group in duplicate_groups.items():
        # Sort group by duration to optimize comparison
        group.sort(key=lambda x: x["meta"]["duration"])

        for i in range(len(group)):
            f1 = group[i]
            if f1["path"] in deleted_paths or not f1["path"].exists():
                continue

            for j in range(i + 1, len(group)):
                f2 = group[j]
                if f2["path"] in deleted_paths or not f2["path"].exists():
                    continue

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

                    if delete["path"] == f1["path"]:
                        break

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

    try:
        SpotdlSync(audio_format=selected_fmt, bitrate=selected_bitrate).run()
    except KeyboardInterrupt:
        print("\n\n👋 Sync interrupted by user. Exiting cleanly.")
        sys.exit(0)

