---
name: sync_playlist
description: Guide the user to sync Spotify playlists or run the downloader
---

# Playlist Syncing Skill

Use this skill when the user requests to sync, download, or configure playlists in the Spotup workspace.

## Step-by-step Execution

1. **Verify Environment Variables**:
   Check if the necessary environment variables are set (either via `.env` or system variables):
   - `BASE_PATH`
   - `SPOTIPY_CLIENT_ID`
   - `SPOTIPY_CLIENT_SECRET`
   - `USERNAME`

2. **Verify FFmpeg**:
   Ensure `ffmpeg` is installed and available in the current environment path.

3. **Check Sync Map**:
   Refer to the [snapshot_map.json](file:///mnt/c/Users/ricky/Documents/Programming/Python/spotup/snapshot_map.json) file to identify already-downloaded playlists and their status.

4. **Run Synchronization**:
   Recommend running the sync tool:
   ```bash
   uv run python main.py
   ```
