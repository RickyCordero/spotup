import os
from pathlib import (
    Path,
)
import json
import random

import spotipy
from spotipy.oauth2 import (
    SpotifyOAuth,
)
from spotdl import Spotdl

BASE_PATH = os.environ.get("BASE_PATH")
NUM_THREADS = 4
MAX_DOWNLOAD_RETRIES = 5

class SpotifyClient():
    def __init__(self):
        self.username = os.environ.get("USERNAME")
        self.scope = "playlist-read-private"
        self.auth_manager = SpotifyOAuth(
            scope=self.scope,
            open_browser=False,
        )
        self.sp = spotipy.Spotify(auth_manager=self.auth_manager)
        self.playlists = None
        self.target_snapshot_map = {}

    def get_playlists(self):
        return self.sp.user_playlists(
            self.username,
            limit=50, # max is 50 records per page
        )

    def set_playlists(self):
        self.playlists = self.get_playlists()

    def view_playlists(self):
        self.set_playlists()
        while self.playlists:
            for i, playlist in enumerate(self.playlists['items']):
                print("%4d %s %s" % (i + 1 + self.playlists['offset'], playlist['uri'],  playlist['name']))
        if self.playlists['next']:
            self.playlists = self.sp.next(self.playlists)
        else:
            self.playlists = None
    
    def build_snapshot_map(self):
        snapshot_map = {}
        playlists = self.sp.user_playlists(self.username)
        while playlists is not None:
            for playlist_obj in playlists.get("items"):
                owner_id = playlist_obj.get("owner").get("id")
                if owner_id == self.username:
                    snapshot_id = playlist_obj.get("snapshot_id")
                    name = playlist_obj.get("name")
                    url = playlist_obj.get("external_urls").get("spotify")
                    snapshot_map[name] = {
                        "url": url,
                        "snapshot_id": snapshot_id,
                    }
            if playlists['next']:
                playlists = self.sp.next(playlists)
            else:
                playlists = None
        return snapshot_map

    def dump_map(self, m):
        with open("snapshot_map.json", "w") as snapshot_file:
            json.dump(m, snapshot_file, indent=4, sort_keys=True)

    def incremental_dump_map(self, entry):
        """
        Parameters
        -----------
            entry: tuple(str, dict)
        """
        with open("snapshot_map.json", "r") as snapshot_file:
            if res := snapshot_file.read():
                data = json.loads(res)
            else:
                data = {}

        k, v = entry
        data[k] = v

        with open("snapshot_map.json", "w") as snapshot_file:
            json.dump(data, snapshot_file, indent=4, sort_keys=True)

    def dump_snapshot_map(self):
        self.dump_map(self.build_snapshot_map())

    def get_snapshot_diff(self):
        playlists_to_update = []
        prev_map = self.load_snapshot_map()
        target_snapshot_map = self.build_snapshot_map()
        count = 0
        for playlist_name in target_snapshot_map.keys():
            playlist_obj = target_snapshot_map.get(playlist_name)
            playlist_snapshot_id = playlist_obj.get("snapshot_id")
            prev_playlist_obj = prev_map.get(playlist_name)
            if prev_playlist_obj:
                prev_playlist_snapshot_id = prev_playlist_obj.get("snapshot_id")
                if playlist_snapshot_id != prev_playlist_snapshot_id:
                    print(f"Playlist '{playlist_name}' was updated. Download again: {playlist_obj}")
                    playlists_to_update.append({"name": playlist_name, "url": playlist_obj.get("url")})
                    count += 1
            else:
                print(f"Playlist '{playlist_name}' was added")
                playlists_to_update.append({"name": playlist_name, "url": playlist_obj.get("url")})
                count += 1
        print(f"{count} playlists should be updated.")
        self.target_snapshot_map = target_snapshot_map
        return playlists_to_update

    def load_snapshot_map(self):
        with open("snapshot_map.json") as snapshot_file:
            if data := snapshot_file.read():
                return json.loads(data)
            else:
                return {}

class SpotdlClient():

    def __init__(self):
        self.spotify_client = SpotifyClient()
        self.spotdl = Spotdl(
            client_id=os.environ.get("SPOTIPY_CLIENT_ID"),
            client_secret=os.environ.get("SPOTIPY_CLIENT_SECRET"),
            # threads=NUM_THREADS,
        )
    
    def download(self):
        playlists_to_update = self.spotify_client.get_snapshot_diff()
        random.shuffle(playlists_to_update)
        retries = MAX_DOWNLOAD_RETRIES
        while retries > 0:
            try:
                for playlist_obj in playlists_to_update:
                    playlist_name = playlist_obj.get("name")
                    output_dir = f'{BASE_PATH}/{playlist_name}'
                    print(f"Looking for folder '{output_dir}'")
                    if not Path(output_dir).exists():
                        print(f"Folder {output_dir} does not exist, creating...")
                        Path(output_dir).mkdir(exist_ok=True)
                        print(f"Successfully created folder '{output_dir}'")
                    playlist_url = playlist_obj.get("url")
                    print(f"Searching for playlist '{playlist_name}' ({playlist_url})")
                    songs = self.spotdl.search([
                        playlist_url,
                    ])
                    print(f"Downloading {len(songs)} songs...")
                    self.spotdl.downloader.settings["output"] = output_dir
                    results = self.spotdl.download_songs(songs)
                    self.spotify_client.incremental_dump_map((
                        playlist_name,
                        self.spotify_client.target_snapshot_map.get(playlist_name),
                    ))
                return
            except Exception as err:
                print(err)
                retries -= 1
            print("Retrying download")

if __name__ == "__main__":
    spotdl_client = SpotdlClient()
    spotdl_client.download()
