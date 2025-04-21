import psycopg2
import requests
from tqdm import tqdm
from config import DB_CONFIG, SEASONS, COMPETITION

# Insert or update player stats per game into player_game_stats table using V3 API
# For each game, extract game_number and retrieve player statistics
# Insert player data including stats, position, and starting_five flag
# Use ON CONFLICT DO UPDATE to update existing records with missing fields (e.g. dorsal)

def insert_player_game_stats():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    with conn.cursor() as cur:
        total_upserts = 0

        # Get gamecode → game_number mapping from database
        cur.execute("SELECT gamecode, game_number FROM games WHERE game_number IS NOT NULL;")
        game_map = dict(cur.fetchall())

        # Iterate through each season to process relevant games
        for season in tqdm(SEASONS, desc="Inserting player stats per season"):
            season_code = f"{COMPETITION}{season}"

            for gamecode, game_number in game_map.items():
                if not gamecode.startswith(season_code):
                    continue

                url = f"https://api-live.euroleague.net/v3/competitions/{COMPETITION}/seasons/{season_code}/games/{game_number}/stats"
                headers = {"Accept": "application/json"}

                try:
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                    data = response.json()
                except requests.RequestException as e:
                    print(f"Failed to retrieve stats for {gamecode}: {e}")
                    continue

                # Check both local and road teams explicitly from data dict
                for side in ["local", "road"]:
                    team_data = data.get(side)
                    if not team_data:
                        continue

                    players = team_data.get("players")
                    if not players:
                        continue

                    # Try to get team_code from the first player's club
                    team_code = players[0].get("player", {}).get("club", {}).get("code")
                    if not team_code:
                        continue

                    for entry in players:
                        player = entry.get("player", {})
                        stats = entry.get("stats", {})
                        person = player.get("person", {})

                        person_code = person.get("code")
                        if not person_code:
                            continue

                        # Safe integer casting
                        def safe_int(val):
                            try:
                                return int(val)
                            except (TypeError, ValueError):
                                return None

                        # Build stat dictionary
                        values = {
                            "gamecode": gamecode,
                            "person_code": person_code,
                            "team_code": team_code,
                            "points": safe_int(stats.get("points")),
                            "minutes_played": safe_int(stats.get("timePlayed")),
                            "pir": safe_int(stats.get("valuation")),
                            "field_goals_2_made": safe_int(stats.get("fieldGoalsMade2")),
                            "field_goals_2_attempted": safe_int(stats.get("fieldGoalsAttempted2")),
                            "field_goals_3_made": safe_int(stats.get("fieldGoalsMade3")),
                            "field_goals_3_attempted": safe_int(stats.get("fieldGoalsAttempted3")),
                            "free_throws_made": safe_int(stats.get("freeThrowsMade")),
                            "free_throws_attempted": safe_int(stats.get("freeThrowsAttempted")),
                            "total_rebounds": safe_int(stats.get("totalRebounds")),
                            "offensive_rebounds": safe_int(stats.get("offensiveRebounds")),
                            "defensive_rebounds": safe_int(stats.get("defensiveRebounds")),
                            "assists": safe_int(stats.get("assistances")),
                            "steals": safe_int(stats.get("steals")),
                            "turnovers": safe_int(stats.get("turnovers")),
                            "blocks_favour": safe_int(stats.get("blocksFavour")),
                            "blocks_against": safe_int(stats.get("blocksAgainst")),
                            "fouls_committed": safe_int(stats.get("foulsCommited")),
                            "fouls_received": safe_int(stats.get("foulsReceived")),
                            "plus_minus": safe_int(stats.get("plusMinus")),
                            "start_five": stats.get("startFive", False),
                            "dorsal": safe_int(stats.get("dorsal")) or safe_int(player.get("dorsal")),
                            "position": safe_int(player.get("position")),
                            "position_name": player.get("positionName"),
                            "starting_five": stats.get("startFive", False)
                        }

                        # Upsert player game stats: insert or update if null
                        cur.execute("""
                            INSERT INTO player_game_stats (
                                gamecode, person_code, team_code, points, minutes_played, pir,
                                field_goals_2_made, field_goals_2_attempted, field_goals_3_made, field_goals_3_attempted,
                                free_throws_made, free_throws_attempted, total_rebounds, offensive_rebounds,
                                defensive_rebounds, assists, steals, turnovers, blocks_favour, blocks_against,
                                fouls_committed, fouls_received, plus_minus, start_five, dorsal, position,
                                position_name, starting_five
                            ) VALUES (
                                %(gamecode)s, %(person_code)s, %(team_code)s, %(points)s, %(minutes_played)s, %(pir)s,
                                %(field_goals_2_made)s, %(field_goals_2_attempted)s, %(field_goals_3_made)s, %(field_goals_3_attempted)s,
                                %(free_throws_made)s, %(free_throws_attempted)s, %(total_rebounds)s, %(offensive_rebounds)s,
                                %(defensive_rebounds)s, %(assists)s, %(steals)s, %(turnovers)s, %(blocks_favour)s, %(blocks_against)s,
                                %(fouls_committed)s, %(fouls_received)s, %(plus_minus)s, %(start_five)s, %(dorsal)s, %(position)s,
                                %(position_name)s, %(starting_five)s
                            )
                            ON CONFLICT (gamecode, person_code)
                            DO UPDATE SET
                                dorsal = COALESCE(EXCLUDED.dorsal, player_game_stats.dorsal),
                                position = COALESCE(EXCLUDED.position, player_game_stats.position),
                                position_name = COALESCE(EXCLUDED.position_name, player_game_stats.position_name),
                                start_five = COALESCE(EXCLUDED.start_five, player_game_stats.start_five),
                                starting_five = COALESCE(EXCLUDED.starting_five, player_game_stats.starting_five);
                        """, values)

                        total_upserts += 1

    conn.close()
    print(f"Insertion complete. Total player stats inserted or updated: {total_upserts}")

if __name__ == "__main__":
    insert_player_game_stats()
