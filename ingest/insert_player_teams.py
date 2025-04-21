import psycopg2
import requests
from tqdm import tqdm
from config import DB_CONFIG, SEASONS, COMPETITION

def insert_player_teams():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    with conn.cursor() as cur:
        total_inserts = 0

        # Get gamecode → game_number mapping only for current SEASONS
        cur.execute("""
            SELECT gamecode, game_number, season_code
            FROM games
            WHERE game_number IS NOT NULL
              AND season_code = ANY(%s)
        """, ([f"{COMPETITION}{s}" for s in SEASONS],))
        game_map = cur.fetchall()

        for gamecode, game_number, season_code in tqdm(game_map, desc="Inserting player-team-season"):
            url = f"https://api-live.euroleague.net/v3/competitions/{COMPETITION}/seasons/{season_code}/games/{game_number}/stats"
            headers = {"Accept": "application/json"}

            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()
            except requests.RequestException as e:
                print(f"Failed to retrieve data for {gamecode}: {e}")
                continue

            for side in ["local", "road"]:
                team_data = data.get(side)
                if not team_data:
                    continue

                players = team_data.get("players")
                if not players:
                    continue

                for entry in players:
                    player = entry.get("player", {})
                    person = player.get("person", {})
                    team = player.get("club", {})

                    person_code = person.get("code")
                    team_code = team.get("code")
                    if not person_code or not team_code:
                        continue

                    def safe_int(val):
                        try:
                            return int(val)
                        except (ValueError, TypeError):
                            return None

                    values = {
                        "person_code": person_code,
                        "team_code": team_code,
                        "season_code": season_code,
                        "jersey_number": safe_int(player.get("dorsal")),
                        "position": safe_int(player.get("position")),
                        "position_name": player.get("positionName")
                    }

                    cur.execute("""
                        INSERT INTO player_teams (
                            person_code, team_code, season_code,
                            jersey_number, position, position_name
                        ) VALUES (
                            %(person_code)s, %(team_code)s, %(season_code)s,
                            %(jersey_number)s, %(position)s, %(position_name)s
                        )
                        ON CONFLICT (person_code, team_code, season_code) DO NOTHING;
                    """, values)

                    total_inserts += 1

    conn.close()
    print(f"Insertion complete. Total player-team-season rows inserted: {total_inserts}")

if __name__ == "__main__":
    insert_player_teams()
