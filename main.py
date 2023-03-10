import os
from pathlib import (
    Path,
)
import json
import spotipy
from spotipy.oauth2 import (
    SpotifyOAuth,
)
from spotdl import Spotdl

BASE_PATH = os.environ.get("BASE_PATH")
NUM_THREADS = 4

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
        self.snapshot_map = {}

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
    
    def set_snapshot_map(self):
        self.snapshot_map = self.build_snapshot_map()

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

    def dump_snapshot_map(self):
        self.dump_map(self.snapshot_map)

    def get_snapshot_diff(self):
        playlists_to_update = []
        try:
            prev_map = self.load_snapshot_map()
        except Exception as err:
            prev_map = None
        curr_map = self.build_snapshot_map()
        count = 0
        if prev_map:
            for k in curr_map.keys():
                curr_obj = curr_map.get(k)
                curr_snapshot_id = curr_obj.get("snapshot_id")
                prev_obj = prev_map.get(k)
                if prev_obj:
                    prev_snapshot_id = prev_obj.get("snapshot_id")
                    if curr_snapshot_id != prev_snapshot_id:
                        print(f"Playlist '{k}' was updated. Download again: {curr_obj}")
                        count += 1
                        playlists_to_update.append({"name": k, "url": curr_obj.get("url")})
                else:
                    print(f"Playlist '{k}' was added/removed")
        print(f"{count} playlists should be updated.")
        self.snapshot_map = curr_map
        return playlists_to_update

    def load_snapshot_map(self):
        with open("snapshot_map.json") as snapshot_file:
            return json.loads(snapshot_file.read())

class SpotdlClient():

    def __init__(self):
        self.spotify_client = SpotifyClient()
        self.spotdl = Spotdl(
            client_id=os.environ.get("SPOTIPY_CLIENT_ID"),
            client_secret=os.environ.get("SPOTIPY_CLIENT_SECRET"),
            threads=NUM_THREADS,
        )
    
    def download(self):
        playlists_to_update = self.spotify_client.get_snapshot_diff()
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
                self.spotdl.downloader.output = output_dir
                results = self.spotdl.download_songs(songs)
            self.spotify_client.dump_map(self.spotify_client.snapshot_map)
        except Exception as err:
            print(err)

if __name__ == "__main__":
    spotdl_client = SpotdlClient()
    spotdl_client.download()
