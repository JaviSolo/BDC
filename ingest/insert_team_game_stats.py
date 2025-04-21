import psycopg2
import requests
import json
from tqdm import tqdm
from config import DB_CONFIG, COMPETITION, SEASONS

# Insert or update team stats per game into team_game_stats table using API V2 (partials) + aggregation from player_game_stats
# For each game: retrieve partials and extra periods from the V2 endpoint
# Then aggregate stats from player_game_stats grouped by team
# Insert or update records in team_game_stats

def insert_team_game_stats():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    with conn.cursor() as cur:
        total_upserts = 0

        # Get all gamecode, game_number pairs from database
        cur.execute("SELECT gamecode, game_number FROM games WHERE game_number IS NOT NULL;")
        game_map = dict(cur.fetchall())

        for season in tqdm(SEASONS, desc="Inserting team game stats per season"):
            season_code = f"{COMPETITION}{season}"

            for gamecode, game_number in game_map.items():
                if not gamecode.startswith(season_code):
                    continue

                # Get partials from API V2
                url = f"https://api-live.euroleague.net/v2/competitions/{COMPETITION}/seasons/{season_code}/games/{game_number}"
                try:
                    response = requests.get(url, headers={"Accept": "application/json"})
                    response.raise_for_status()
                    game_data = response.json()
                except requests.RequestException as e:
                    print(f"Failed to fetch game data for {gamecode}: {e}")
                    continue

                game_teams = {
                    "local": game_data.get("local", {}),
                    "road": game_data.get("road", {})
                }

                for side, team_data in game_teams.items():
                    club = team_data.get("club", {})
                    team_code = club.get("code")
                    if not team_code:
                        continue

                    # Get partials
                    partials = team_data.get("partials", {})
                    points_q1 = partials.get("partials1")
                    points_q2 = partials.get("partials2")
                    points_q3 = partials.get("partials3")
                    points_q4 = partials.get("partials4")
                    extra_periods = partials.get("extraPeriods") or {}

                    # Aggregate stats from player_game_stats
                    cur.execute("""
                        SELECT
                            COALESCE(SUM(points), 0),
                            COALESCE(SUM(pir), 0),
                            COALESCE(SUM(field_goals_2_made), 0),
                            COALESCE(SUM(field_goals_2_attempted), 0),
                            COALESCE(SUM(field_goals_3_made), 0),
                            COALESCE(SUM(field_goals_3_attempted), 0),
                            COALESCE(SUM(free_throws_made), 0),
                            COALESCE(SUM(free_throws_attempted), 0),
                            COALESCE(SUM(field_goals_2_made + field_goals_3_made), 0),
                            COALESCE(SUM(field_goals_2_attempted + field_goals_3_attempted), 0),
                            COALESCE(SUM(total_rebounds), 0),
                            COALESCE(SUM(defensive_rebounds), 0),
                            COALESCE(SUM(offensive_rebounds), 0),
                            COALESCE(SUM(assists), 0),
                            COALESCE(SUM(steals), 0),
                            COALESCE(SUM(turnovers), 0),
                            COALESCE(SUM(blocks_favour), 0),
                            COALESCE(SUM(blocks_against), 0),
                            COALESCE(SUM(fouls_committed), 0),
                            COALESCE(SUM(fouls_received), 0),
                            COALESCE(SUM(plus_minus), 0),
                            COALESCE(SUM(minutes_played), 0)
                        FROM player_game_stats
                        WHERE gamecode = %s AND team_code = %s;
                    """, (gamecode, team_code))

                    stats = cur.fetchone()

                    values = {
                        "gamecode": gamecode,
                        "team_code": team_code,
                        "points": stats[0],
                        "valuation": stats[1],
                        "field_goals_2_made": stats[2],
                        "field_goals_2_attempted": stats[3],
                        "field_goals_3_made": stats[4],
                        "field_goals_3_attempted": stats[5],
                        "free_throws_made": stats[6],
                        "free_throws_attempted": stats[7],
                        "field_goals_total_made": stats[8],
                        "field_goals_total_attempted": stats[9],
                        "total_rebounds": stats[10],
                        "defensive_rebounds": stats[11],
                        "offensive_rebounds": stats[12],
                        "assists": stats[13],
                        "steals": stats[14],
                        "turnovers": stats[15],
                        "blocks_favour": stats[16],
                        "blocks_against": stats[17],
                        "fouls_committed": stats[18],
                        "fouls_received": stats[19],
                        "plus_minus": stats[20],
                        "time_played": stats[21],
                        "points_q1": points_q1,
                        "points_q2": points_q2,
                        "points_q3": points_q3,
                        "points_q4": points_q4,
                        "extra_periods": json.dumps(extra_periods)
                    }

                    cur.execute("""
                        INSERT INTO team_game_stats (
                            gamecode, team_code, points, valuation,
                            field_goals_2_made, field_goals_2_attempted,
                            field_goals_3_made, field_goals_3_attempted,
                            free_throws_made, free_throws_attempted,
                            field_goals_total_made, field_goals_total_attempted,
                            total_rebounds, defensive_rebounds, offensive_rebounds,
                            assists, steals, turnovers, blocks_favour, blocks_against,
                            fouls_committed, fouls_received, plus_minus, time_played,
                            points_q1, points_q2, points_q3, points_q4, extra_periods
                        ) VALUES (
                            %(gamecode)s, %(team_code)s, %(points)s, %(valuation)s,
                            %(field_goals_2_made)s, %(field_goals_2_attempted)s,
                            %(field_goals_3_made)s, %(field_goals_3_attempted)s,
                            %(free_throws_made)s, %(free_throws_attempted)s,
                            %(field_goals_total_made)s, %(field_goals_total_attempted)s,
                            %(total_rebounds)s, %(defensive_rebounds)s, %(offensive_rebounds)s,
                            %(assists)s, %(steals)s, %(turnovers)s, %(blocks_favour)s, %(blocks_against)s,
                            %(fouls_committed)s, %(fouls_received)s, %(plus_minus)s, %(time_played)s,
                            %(points_q1)s, %(points_q2)s, %(points_q3)s, %(points_q4)s, %(extra_periods)s
                        )
                        ON CONFLICT (gamecode, team_code) DO UPDATE SET
                            points = EXCLUDED.points,
                            valuation = EXCLUDED.valuation,
                            field_goals_2_made = EXCLUDED.field_goals_2_made,
                            field_goals_2_attempted = EXCLUDED.field_goals_2_attempted,
                            field_goals_3_made = EXCLUDED.field_goals_3_made,
                            field_goals_3_attempted = EXCLUDED.field_goals_3_attempted,
                            free_throws_made = EXCLUDED.free_throws_made,
                            free_throws_attempted = EXCLUDED.free_throws_attempted,
                            field_goals_total_made = EXCLUDED.field_goals_total_made,
                            field_goals_total_attempted = EXCLUDED.field_goals_total_attempted,
                            total_rebounds = EXCLUDED.total_rebounds,
                            defensive_rebounds = EXCLUDED.defensive_rebounds,
                            offensive_rebounds = EXCLUDED.offensive_rebounds,
                            assists = EXCLUDED.assists,
                            steals = EXCLUDED.steals,
                            turnovers = EXCLUDED.turnovers,
                            blocks_favour = EXCLUDED.blocks_favour,
                            blocks_against = EXCLUDED.blocks_against,
                            fouls_committed = EXCLUDED.fouls_committed,
                            fouls_received = EXCLUDED.fouls_received,
                            plus_minus = EXCLUDED.plus_minus,
                            time_played = EXCLUDED.time_played,
                            points_q1 = EXCLUDED.points_q1,
                            points_q2 = EXCLUDED.points_q2,
                            points_q3 = EXCLUDED.points_q3,
                            points_q4 = EXCLUDED.points_q4,
                            extra_periods = EXCLUDED.extra_periods;
                    """, values)

                    total_upserts += 1

    conn.close()
    print(f"Insertion complete. Total team stats inserted or updated: {total_upserts}")

if __name__ == "__main__":
    insert_team_game_stats()
