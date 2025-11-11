import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple, Union

import spotipy
import spotipy.util as util

# Configure a basic logger for the main thread messages
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Type definitions for clarity
ArtistData = Dict[str, Union[str, List[str]]]
SongData = Dict[str, str]
# New type for worker return value: (result_data, log_messages)
WorkerResult = Tuple[Union[ArtistData, List[SongData], None], List[str]]


def main():
    """Main execution flow for creating the Spotify playlist."""

    if len(sys.argv) < 2:
        logging.error("Usage: python script_name.py <spotify_username>")
        sys.exit(1)

    try:
        with open("coachella2026.txt", "r") as file:
            artists = [artist.strip() for artist in file if artist.strip()]
    except FileNotFoundError:
        logging.error("Input file 'coachella2026.txt' not found. Please create it.")
        sys.exit(1)

    playlist_name = "Coachella 2026"
    username = sys.argv[1]
    scope = "playlist-modify-public"

    # Important: Ensure CLIENT_ID and CLIENT_SECRET are set as environment variables
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    redirect_uri = "https://google.com"

    if not client_id or not client_secret:
        logging.error("CLIENT_ID and CLIENT_SECRET environment variables must be set.")
        sys.exit(1)

    token = util.prompt_for_user_token(
        username,
        scope,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
    )

    if not token:
        logging.error(f"Cannot get token for {username}. Check permissions or credentials.")
        return

    sp = spotipy.Spotify(auth=token)

    remixes_allowed_genres = ["house", "dub", "trance", "breakbeat", "bass", "techno", "edm", "dance"]

    # Global list to hold all collected log messages for sequential printing
    all_logs = []

    with ThreadPoolExecutor(max_workers=5) as executor:

        # --- PHASE 1: Fetching Artist Details ---
        logging.info("\n--- PHASE 1: Fetching Artist IDs and Genres (Parallel) ---")

        artist_details_results = list(executor.map(lambda name: fetch_artist_details(sp, name), artists))

        valid_artists = []
        for result in artist_details_results:
            artist_data, logs = result
            all_logs.extend(logs)
            if artist_data is not None:
                valid_artists.append(artist_data)

        # Print logs for Phase 1 sequentially
        for log in all_logs:
            print(log)
        all_logs.clear()  # Clear logs for the next phase

        if not valid_artists:
            logging.warning("No valid artists found on Spotify. Exiting.")
            return

        # --- PHASE 2: Fetching Top Songs ---
        logging.info("\n--- PHASE 2: Fetching Top Songs (Parallel) ---")

        song_data_results = list(
            executor.map(lambda details: fetch_top_songs(sp, details, remixes_allowed_genres), valid_artists)
        )

        all_song_details: List[SongData] = []
        for result in song_data_results:
            artist_songs, logs = result
            all_logs.extend(logs)
            for song in artist_songs:
                all_song_details.append(song)

        # Print logs for Phase 2 sequentially
        for log in all_logs:
            print(log)

        if not all_song_details:
            logging.warning("No songs found for any of the artists. Exiting.")
            return

    # --- PHASE 3: Playlist Creation ---
    song_ids = [song["id"] for song in all_song_details]

    logging.info("\n--- PHASE 3: Creating Playlist and Adding Tracks (Batch) ---")

    try:
        playlist = sp.user_playlist_create(username, playlist_name, public=True)
        logging.info(f"Successfully created playlist: {playlist_name}")

        add_tracks_in_batches(sp, username, playlist["id"], song_ids)

        logging.info(f"\nPlaylist '{playlist_name}' was created successfully with {len(song_ids)} songs 🎉")

    except spotipy.exceptions.SpotifyException as e:
        logging.error(f"Spotify API Error during playlist creation or track addition: {e}")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def fetch_artist_details(sp: spotipy.Spotify, artist_name: str) -> WorkerResult:
    """Worker function to fetch Spotify ID and genre for a single artist, returning logs."""
    logs = []

    # Normalize the search query for comparison
    normalized_query = artist_name.lower()

    try:
        # Step 1: Search for top 5 results instead of just 1
        results = sp.search(artist_name, limit=5, type="artist")
        items = results["artists"]["items"]

        if not items:
            logs.append(f"[WARNING] Artist: '{artist_name}' not found on Spotify. Skipping.")
            return None, logs

        best_match_item = None

        # Step 2: Iterate through results to find the most accurate match
        for item in items:
            spotify_name = item["name"]

            # Simple, robust check: Case-insensitive exact match
            if spotify_name.lower() == normalized_query:
                best_match_item = item
                break

            # Fallback/Fuzzy check (Optional, but can help with minor stylization like "The xx" vs "The XX")
            # We'll use a high similarity threshold for safety.
            similarity = difflib.SequenceMatcher(None, normalized_query, spotify_name.lower()).ratio()
            if similarity > 0.90:  # 90% match threshold
                best_match_item = item
                break

            # Special case for "The" prefix removal (e.g., "The xx" vs "xx")
            if normalized_query.startswith("the ") and spotify_name.lower() == normalized_query[4:]:
                best_match_item = item
                break

        if best_match_item is None:
            # If no good match is found in the top 5, log the top (wrong) result for transparency
            top_result_name = items[0]["name"]
            logs.append(
                f"[WARNING] Artist: '{artist_name}' has no close match in top 5. Top result was '{top_result_name}'. Skipping."
            )
            return None, logs

        # Step 3: Process the best match
        artist_id = best_match_item["id"]
        artist_genres = best_match_item["genres"]
        spotify_name = best_match_item["name"]

        logs.append(f"--- Artist Details: {artist_name} ---")
        logs.append(f"  > Matched Spotify Name: {spotify_name}")
        logs.append(f"  > ID: {artist_id}")
        logs.append(f"  > Genres: {', '.join(artist_genres) if artist_genres else 'None'}")

        return {
            "name": spotify_name,
            "id": artist_id,
            "genres": artist_genres,
        }, logs

    except Exception as e:
        logs.append(f"[ERROR] Fetching details for '{artist_name}' failed: {e}. Skipping.")
        return None, logs


def fetch_top_songs(sp: spotipy.Spotify, artist_details: ArtistData, remixes_allowed: List[str]) -> WorkerResult:
    """Worker function to grab a list of top 5 songs for a single artist, returning songs and logs."""

    artist_name = artist_details["name"]
    artist_id = artist_details["id"]
    artist_genres = artist_details["genres"]

    tracks: List[SongData] = []
    logs = [f"\n--- Top Songs: {artist_name} ({artist_id}) ---"]

    allow_remixes_flag = any(remix_genre in genre for genre in artist_genres for remix_genre in remixes_allowed)
    logs.append(f"  > Remix Check: Genres permit remixes/edits? {allow_remixes_flag}")

    try:
        # Fetch top 10 tracks
        top_tracks_result = sp.artist_top_tracks(artist_id)
        all_tracks = top_tracks_result["tracks"]

        if not all_tracks:
            logs.append(f"  > WARNING: No top tracks available for {artist_name}.")
            return [], logs

        tracks_to_add = []

        # Iterate up to 10 tracks to find the required 5
        for i, track in enumerate(all_tracks[:10]):
            song_name = track["name"]
            track_id = track["id"]

            # Check for generic remix/edit terms
            is_remix = "remix" in song_name.lower() or (
                "edit" in song_name.lower()
                and "radio edit" not in song_name.lower()
                and "album edit" not in song_name.lower()
            )

            if allow_remixes_flag or not is_remix:
                # Song is added
                tracks_to_add.append(track)
                logs.append(f"  > ADDED: '{song_name}' | ID: {track_id}")
            else:
                # Song is skipped
                logs.append(f"  > SKIPPED: '{song_name}' (Remix/Edit detected and not allowed by genre.)")

            if len(tracks_to_add) >= 5:
                break

        # Finalize the list for return
        for track in tracks_to_add[:5]:
            tracks.append(
                {
                    "id": track["id"],
                    "name": track["name"],
                    "artist_name": artist_name,
                    "artist_id": artist_id,
                }
            )

        logs.append(f"  > Final Count: {len(tracks)} songs added for {artist_name}")

        return tracks, logs

    except spotipy.exceptions.SpotifyException as e:
        logs.append(f"[ERROR] Spotify API Error fetching tracks for {artist_name}: {e}. Skipping.")
        return [], logs
    except Exception as e:
        logs.append(f"[ERROR] An unexpected error occurred fetching tracks for {artist_name}: {e}. Skipping.")
        return [], logs


def add_tracks_in_batches(sp: spotipy.Spotify, username: str, playlist_id: str, song_ids: List[str]):
    """Adds tracks to a playlist in batches of up to 100 for efficiency."""

    batch_size = 100

    for i in range(0, len(song_ids), batch_size):
        batch = song_ids[i : i + batch_size]

        sp.user_playlist_add_tracks(username, playlist_id, batch)
        logging.info(f"Added batch {int(i/batch_size) + 1} ({len(batch)} tracks) to the playlist.")


if __name__ == "__main__":
    main()
