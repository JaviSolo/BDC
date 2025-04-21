import psycopg2
import requests
from config import DB_CONFIG, SEASONS, COMPETITION

def insert_venues_all_seasons():
    inserted_venues = set()

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    with conn.cursor() as cur:
        for year in SEASONS:
            season_code = f"{COMPETITION}{year}"
            url = f"https://api-live.euroleague.net/v2/competitions/{COMPETITION}/seasons/{season_code}/venues"

            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

                print(f"[{season_code}] {len(data)} clubs found")

                for club in data:
                    for venue in club.get("venues", []):
                        venue_code = venue.get("code")
                        if not venue_code or venue_code in inserted_venues:
                            continue

                        inserted_venues.add(venue_code)

                        name = venue.get("name")
                        capacity = venue.get("capacity")
                        address = venue.get("address")
                        active = venue.get("active")
                        notes = venue.get("notes")
                        images = venue.get("images", {}).get("medium")

                        cur.execute(
                            """
                            INSERT INTO venues (
                                venue_code, name, capacity, address, active, notes, images
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (venue_code) DO NOTHING;
                            """,
                            (venue_code, name, capacity, address, active, notes, images)
                        )

            except requests.RequestException as e:
                print(f"Error fetching venues for season {season_code}: {e}")

    print(f"{len(inserted_venues)} total unique venues inserted.")
    conn.close()

if __name__ == "__main__":
    insert_venues_all_seasons()
