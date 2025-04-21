import os
import requests
import psycopg2
from tqdm import tqdm
from ingest.config import DB_CONFIG

# Constants
COMPETITION = "E"
SEASONS = [f"E{year}" for year in range(2000, 2031)]

# Destination folder for player images
DEST_FOLDER = "app/static/images/people"
os.makedirs(DEST_FOLDER, exist_ok=True)

def insert_player_images():
    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    with conn.cursor() as cur:
        # Fetch all games from the database
        cur.execute("""
            SELECT season_code, gamecode
            FROM games
            WHERE season_code = ANY(%s)
        """, (SEASONS,))
        games = cur.fetchall()

        with tqdm(total=len(games), desc="Processing games") as pbar:
            for season_code, gamecode in games:
                game_number = gamecode.split("_")[-1]
                url = f"https://api-live.euroleague.net/v3/competitions/{COMPETITION}/seasons/{season_code}/games/{game_number}/stats"

                try:
                    response = requests.get(url, headers={"Accept": "application/json"}, timeout=10)
                    response.raise_for_status()
                    data = response.json()
                except Exception as e:
                    print(f"[✘] Failed to fetch game {gamecode}: {e}")
                    pbar.update(1)
                    continue

                inserted_count = 0

                for side in ["local", "road"]:
                    players = data.get(side, {}).get("players", [])

                    for player_data in players:
                        player = player_data.get("player", {})
                        person = player.get("person", {})
                        images = player.get("images", {})
                        person_code = person.get("code")

                        if not person_code:
                            continue

                        for context in ["headshot", "action"]:
                            image_url = images.get(context)
                            if not image_url:
                                continue

                            filename = f"{person_code}_{season_code}_{context}.jpg"
                            file_path = f"/images/people/{filename}"
                            full_path = os.path.join(DEST_FOLDER, filename)

                            if not os.path.exists(full_path):
                                try:
                                    img_response = requests.get(image_url, timeout=10)
                                    img_response.raise_for_status()
                                    with open(full_path, "wb") as f:
                                        f.write(img_response.content)
                                    print(f"[✔] Downloaded {filename}")
                                except Exception as e:
                                    print(f"[✘] Failed to download {filename}: {e}")
                                    continue

                            cur.execute("""
                                SELECT 1 FROM images_people
                                WHERE person_code = %s AND season_code = %s AND context = %s
                            """, (person_code, season_code, context))

                            if not cur.fetchone():
                                cur.execute("""
                                    INSERT INTO images_people (person_code, season_code, context, file_path)
                                    VALUES (%s, %s, %s, %s)
                                """, (person_code, season_code, context, file_path))
                                inserted_count += 1
                                print(f"[+] Inserted: {filename}")
                            else:
                                print(f"[→] Already registered: {filename}")

                print(f"[✔] Game {gamecode}: Inserted {inserted_count} new images")
                pbar.update(1)

    conn.close()
    print("Finished.")

if __name__ == "__main__":
    insert_player_images()
