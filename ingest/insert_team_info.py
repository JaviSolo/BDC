import psycopg2
import requests
from tqdm import tqdm
from config import DB_CONFIG

# Insert or update team descriptions into team_info table using V3 API
# For each team in the database, fetch its info from the API and insert/update the description

def insert_team_info():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    with conn.cursor() as cur:
        total_upserts = 0

        # Get all team codes from the teams table
        cur.execute("SELECT team_code FROM teams;")
        team_codes = [row[0] for row in cur.fetchall()]

        for team_code in tqdm(team_codes, desc="Inserting team info"):
            url = f"https://api-live.euroleague.net/v3/clubs/{team_code}/info"
            try:
                response = requests.get(url, headers={"Accept": "application/json"})
                response.raise_for_status()
                data = response.json()
                description = data.get("info")

                if not description:
                    continue

                cur.execute("""
                    INSERT INTO team_info (team_code, description)
                    VALUES (%s, %s)
                    ON CONFLICT (team_code) DO UPDATE SET
                        description = EXCLUDED.description;
                """, (team_code, description))

                total_upserts += 1

            except requests.RequestException as e:
                print(f"Failed to retrieve info for team {team_code}: {e}")
                continue

    conn.close()
    print(f"Insertion complete. Total team info inserted or updated: {total_upserts}")

if __name__ == "__main__":
    insert_team_info()
