import psycopg2
import requests
from tqdm import tqdm
from config import DB_CONFIG, SEASONS, COMPETITION

# Insert head coaches and staff per season/team into coach_teams table
# Uses V2 API endpoint: /v2/competitions/{competitionCode}/seasons/{seasonCode}/clubs/{teamCode}/people
# Only inserts records where typeName is Coach, Assistant Coach, etc.
# Ensures that coach exists in people table before inserting into coach_teams

def insert_coach_teams():

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    with conn.cursor() as cur:
        total_inserted = 0

        for season in tqdm(SEASONS, desc="Inserting coach_teams per season"):
            full_season_code = f"{COMPETITION}{season}"

            # Get all team codes that actually played in this season (either home or away)
            cur.execute("""
                SELECT DISTINCT home_team_code FROM games WHERE season_code = %s
                UNION
                SELECT DISTINCT away_team_code FROM games WHERE season_code = %s
            """, (full_season_code, full_season_code))
            teams = [row[0] for row in cur.fetchall()]

            for team_code in teams:
                url = f"https://api-live.euroleague.net/v2/competitions/{COMPETITION}/seasons/{full_season_code}/clubs/{team_code}/people"
                headers = {"Accept": "application/json"}

                try:
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                    data = response.json()
                    people = data if isinstance(data, list) else data.get("data", [])
                except requests.HTTPError as e:
                    print(f"Error retrieving people for team {team_code}, season {full_season_code}: {e}")
                    continue
                except Exception as e:
                    print(f"Unexpected error for team {team_code}, season {full_season_code}: {e}")
                    continue

                for person in people:
                    person_data = person.get("person", {})
                    person_code = person_data.get("code")
                    role_type = person.get("typeName")  # e.g., "Coach", "Assistant Coach", etc.

                    # Skip if any required field is missing
                    if not person_code or not role_type:
                        continue

                    # Exclude players
                    if role_type == "Player":
                        continue

                    # Check if coach exists in people table
                    cur.execute("SELECT 1 FROM people WHERE person_code = %s", (person_code,))
                    exists = cur.fetchone()
                    if not exists:
                        continue  # Coach not in people table, skip

                    # Insert into coach_teams
                    cur.execute(
                        """
                        INSERT INTO coach_teams (person_code, team_code, season_code, role)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (person_code, team_code, season_code) DO NOTHING;
                        """,
                        (person_code, team_code, full_season_code, role_type)
                    )
                    total_inserted += 1

    conn.close()
    print(f"Insertion complete. Total coach-team assignments inserted: {total_inserted}")

if __name__ == "__main__":
    insert_coach_teams()
