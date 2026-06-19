# Spotup Project Rules and Guidelines

Welcome to the **Spotup** workspace. This project is a Python tool designed to sync Spotify playlists to a local storage location using `spotipy` and `spotdl`.

## Rules & Behavioral Constraints

1. **Anti-Rate Limit Mode**: Always enforce strict rate limiting logic when interacting with Spotify and YouTube APIs. Do not increase the `NUM_THREADS` beyond 1 in [main.py](file:///mnt/c/Users/ricky/Documents/Programming/Python/spotup/main.py) unless explicitly requested.
2. **Environment Variables**: The application relies on:
   - `BASE_PATH`: Resolves to the music destination directory.
   - `SPOTIPY_CLIENT_ID`: Spotify Developer Client ID.
   - `SPOTIPY_CLIENT_SECRET`: Spotify Developer Client Secret.
   - `USERNAME`: Spotify Username.
   Ensure these are always loaded/validated correctly.
3. **FFmpeg Requirement**: SpotDL requires FFmpeg to tag audio files. Ensure check/validation for FFmpeg is preserved.
4. **Code Quality**:
   - Maintain documentation integrity. Keep comments and docstrings.
   - Ensure exceptions (especially rate limit HTTP 429) are gracefully handled and terminate execution to prevent ban.

## Useful Resources & Files
- [main.py](file:///mnt/c/Users/ricky/Documents/Programming/Python/spotup/main.py): Entry point for playlist syncing logic.
- [Pipfile](file:///mnt/c/Users/ricky/Documents/Programming/Python/spotup/Pipfile): Python packaging & dependencies.
- [snapshot_map.json](file:///mnt/c/Users/ricky/Documents/Programming/Python/spotup/snapshot_map.json): Stores snapshots of synced playlists to optimize future sync operations.
