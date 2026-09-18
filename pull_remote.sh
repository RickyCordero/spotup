#!/usr/bin/env bash
# pull_remote.sh
# Pulls music from the Digital Ocean server back into the local BASE_PATH.
# Safe: uses --ignore-existing so local files are NEVER overwritten.
# Only files present on the server but missing locally will be copied down.

set -euo pipefail

# --- Config ---
REMOTE_USER="root"
REMOTE_HOST="104.236.89.173"
REMOTE_MUSIC_DIR="/home/ricky/Dropbox/Engine Library/Music/"
LOCAL_MUSIC_DIR="/mnt/e/Music/"

# Path to your OpenSSH private key (converted from privatekey.ppk — see below if needed)
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_ed25519}"

# Directories to exclude from the pull
EXCLUDE_DIRS=()

# --- Validate ---
if [ ! -f "$SSH_KEY" ]; then
    echo ""
    echo "❌ SSH key not found at: $SSH_KEY"
    echo ""
    echo "To convert your PuTTY key to OpenSSH format, run:"
    echo "  sudo apt install putty-tools"
    echo "  puttygen ~/.ssh/privatekey.ppk -O private-openssh -o ~/.ssh/id_rsa_do"
    echo "  chmod 600 ~/.ssh/id_rsa_do"
    echo ""
    echo "Then re-run: bash pull_remote.sh"
    exit 1
fi

if [ ! -d "$LOCAL_MUSIC_DIR" ]; then
    echo "❌ Local music directory not found: $LOCAL_MUSIC_DIR"
    exit 1
fi

# --- Build exclude args ---
EXCLUDE_ARGS=()
for dir in "${EXCLUDE_DIRS[@]}"; do
    EXCLUDE_ARGS+=(--exclude="$dir")
done

# --- Dry run first ---
echo ""
echo "=========================================================="
echo "  REMOTE → LOCAL MUSIC PULL"
echo "=========================================================="
echo "  FROM: ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_MUSIC_DIR}"
echo "  TO:   ${LOCAL_MUSIC_DIR}"
echo "  MODE: Safe pull — local files are never overwritten"
echo "=========================================================="
echo ""
echo "🔍 Running dry-run first to show what would be pulled..."
echo ""

rsync --dry-run \
    -av \
    --ignore-existing \
    --progress \
    -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=no" \
    "${EXCLUDE_ARGS[@]}" \
    "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_MUSIC_DIR}" \
    "${LOCAL_MUSIC_DIR}"

echo ""
read -rp "▶  Proceed with actual pull? (y/N): " confirm
if [[ "${confirm,,}" != "y" ]]; then
    echo "Aborted."
    exit 0
fi

echo ""
echo "📥 Pulling missing tracks from server..."
echo ""

rsync \
    -av \
    --ignore-existing \
    --progress \
    -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=no" \
    "${EXCLUDE_ARGS[@]}" \
    "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_MUSIC_DIR}" \
    "${LOCAL_MUSIC_DIR}"

echo ""
echo "✅ Pull complete. Check /mnt/e/Music/ for restored tracks."
