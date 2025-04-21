import psycopg2
import requests
from tqdm import tqdm
from config import DB_CONFIG, SEASONS, COMPETITION

# Insert referees per game into the game_referees table using V2 API
# For each game, extract referee1, referee2, referee3, referee4
# If referee is present and not found in people table, insert into people using available data
# Then insert (gamecode, person_code, role) into game_referees
# referee1 -> role = 'main', referee2/3/4 -> role = 'assistant'

def insert_game_referees():

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    with conn.cursor() as cur:
        total_inserted = 0

        # Iterate through each season to extract game data
        for season in tqdm(SEASONS, desc="Inserting referees per season"):
            full_season_code = f"{COMPETITION}{season}"
            base_url = f"https://api-live.euroleague.net/v2/competitions/{COMPETITION}/seasons/{full_season_code}/games"
            headers = {"Accept": "application/json"}

            offset = 0
            limit = 500

            while True:
                url = f"{base_url}?limit={limit}&offset={offset}"

                try:
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                    games = response.json().get("data", [])
                    if not games:
                        break
                except requests.RequestException as e:
                    print(f"Error retrieving games for {full_season_code}: {e}")
                    break

                for game in games:
                    gamecode = game.get("identifier")

                    # Check referees 1 to 4
                    for ref_num in range(1, 5):
                        referee = game.get(f"referee{ref_num}")
                        if not referee:
                            continue

                        person_code = referee.get("code")
                        role = "main" if ref_num == 1 else "assistant"

                        # Check if referee exists in people table
                        cur.execute("SELECT 1 FROM people WHERE person_code = %s", (person_code,))
                        exists = cur.fetchone()

                        if not exists:
                            # Extract only the allowed fields in the current schema of 'people'
                            name = referee.get("name")
                            alias = referee.get("alias")

                            country = referee.get("country")
                            country_code = country.get("code") if isinstance(country, dict) else None
                            country_name = country.get("name") if isinstance(country, dict) else None

                            image_url = referee.get("images", {}).get("verticalSmall")

                            # Insert person with only existing fields in table
                            cur.execute(
                                """
                                INSERT INTO people (
                                    person_code, name, alias, country_code, country_name, image_url, is_referee
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (person_code) DO NOTHING;
                                """,
                                (person_code, name, alias, country_code, country_name, image_url, True)
                            )

                        # Insert into game_referees table
                        cur.execute(
                            """
                            INSERT INTO game_referees (gamecode, person_code, role)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (gamecode, person_code) DO NOTHING;
                            """,
                            (gamecode, person_code, role)
                        )
                        total_inserted += 1

                offset += limit

    conn.close()
    print(f"Insertion complete. Total referees inserted: {total_inserted}")

if __name__ == "__main__":
    insert_game_referees()
