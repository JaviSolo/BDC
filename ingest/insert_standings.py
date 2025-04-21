# insert_standings.py

import requests
import psycopg2
from tqdm import tqdm
import json
from config import DB_CONFIG, SEASONS, COMPETITION

# ----------------------
# Database connection
# ----------------------
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# ----------------------
# Extract standings from API
# ----------------------
def fetch_standings(season_code, round_number):
    url = f"https://api-live.euroleague.net/v3/competitions/{COMPETITION}/seasons/{COMPETITION}{season_code}/rounds/{round_number}/calendarstandings"
    response = requests.get(url)
    if response.status_code == 404:
        return []  # Round does not exist for this season
    response.raise_for_status()
    return response.json().get("teams", [])

# ----------------------
# Parse single team entry
# ----------------------
def parse_team(team):
    return {
        "position": team.get("position"),
        "position_change": team.get("positionChange"),
        "games_played": team.get("gamesPlayed"),
        "games_won": team.get("gamesWon"),
        "games_lost": team.get("gamesLost"),
        "qualified": team.get("qualified"),
        "group_name": team.get("groupName"),
        "team_code": team.get("club", {}).get("code"),
        "streaks": team.get("streaks") if team.get("streaks") else []
    }

# ----------------------
# Insert into standings table
# ----------------------
def insert_team_standing(cursor, season_code, round_number, data):
    cursor.execute("""
        INSERT INTO standings (
            season_code, round_number, team_code, position, position_change,
            games_played, games_won, games_lost, qualified, group_name, streaks
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (season_code, round_number, team_code) DO UPDATE SET
            position = EXCLUDED.position,
            position_change = EXCLUDED.position_change,
            games_played = EXCLUDED.games_played,
            games_won = EXCLUDED.games_won,
            games_lost = EXCLUDED.games_lost,
            qualified = EXCLUDED.qualified,
            group_name = EXCLUDED.group_name,
            streaks = EXCLUDED.streaks;
    """, (
        f"{COMPETITION}{season_code}",
        round_number,
        data["team_code"],
        data["position"],
        data["position_change"],
        data["games_played"],
        data["games_won"],
        data["games_lost"],
        data["qualified"],
        data["group_name"],
        json.dumps(data["streaks"])
    ))

# ----------------------
# Main process
# ----------------------
def main():
    with get_connection() as conn:
        with conn.cursor() as cur:
            for season in tqdm(SEASONS, desc="Processing seasons"):
                try:
                    cur.execute("""
                        SELECT DISTINCT round_number
                        FROM games
                        WHERE season_code = %s
                          AND competition_code = %s
                        ORDER BY round_number
                    """, (f"{COMPETITION}{season}", COMPETITION))
                    rounds = [r[0] for r in cur.fetchall() if r[0] is not None]

                    for round_number in rounds:
                        try:
                            teams = fetch_standings(season, round_number)
                            for team in teams:
                                data = parse_team(team)
                                insert_team_standing(cur, season, round_number, data)
                            conn.commit()
                        except Exception as e:
                            print(f"Error on season {season}, round {round_number}: {e}")
                            conn.rollback()
                except Exception as e:
                    print(f"Error loading rounds for season {season}: {e}")
                    conn.rollback()

if __name__ == "__main__":
    main()
