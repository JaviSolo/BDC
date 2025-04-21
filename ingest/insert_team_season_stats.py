import psycopg2
import requests
from tqdm import tqdm
from config import DB_CONFIG, SEASONS, COMPETITION

# Insert or update team stats per season into team_season_stats table using V3 API
# For each team in each season (based on actual games played), retrieve aggregated statistics from the API

def insert_team_season_stats():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    with conn.cursor() as cur:
        total_inserted = 0

        for season in tqdm(SEASONS, desc="Inserting team season stats"):
            season_code = f"{COMPETITION}{season}"

            # Get teams that actually played that season
            cur.execute("""
                SELECT DISTINCT team_code
                FROM (
                    SELECT home_team_code AS team_code FROM games WHERE season_code = %s
                    UNION
                    SELECT away_team_code AS team_code FROM games WHERE season_code = %s
                ) AS season_teams;
            """, (season_code, season_code))

            teams = [row[0] for row in cur.fetchall()]

            for team_code in teams:
                url = f"https://api-live.euroleague.net/v3/competitions/{COMPETITION}/seasons/{season_code}/clubs/{team_code}/stats"
                headers = {"Accept": "application/json"}

                try:
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                    data = response.json()
                    stats = data[0].get("accumulated", {})
                except Exception as e:
                    print(f"⚠️ Failed to get stats for {season_code}, {team_code}: {e}")
                    continue

                # Safe integer casting
                def safe_int(val):
                    try:
                        return int(val)
                    except (TypeError, ValueError):
                        return None

                values = {
                    "season_code": season_code,
                    "team_code": team_code,
                    "games_played": safe_int(stats.get("gamesPlayed")),
                    "points": safe_int(stats.get("points")),
                    "valuation": safe_int(stats.get("valuation")),
                    "field_goals_2_made": safe_int(stats.get("fieldGoalsMade2")),
                    "field_goals_2_attempted": safe_int(stats.get("fieldGoalsAttempted2")),
                    "field_goals_3_made": safe_int(stats.get("fieldGoalsMade3")),
                    "field_goals_3_attempted": safe_int(stats.get("fieldGoalsAttempted3")),
                    "free_throws_made": safe_int(stats.get("freeThrowsMade")),
                    "free_throws_attempted": safe_int(stats.get("freeThrowsAttempted")),
                    "field_goals_total_made": safe_int(stats.get("fieldGoalsMadeTotal")),
                    "field_goals_total_attempted": safe_int(stats.get("fieldGoalsAttemptedTotal")),
                    "total_rebounds": safe_int(stats.get("totalRebounds")),
                    "defensive_rebounds": safe_int(stats.get("defensiveRebounds")),
                    "offensive_rebounds": safe_int(stats.get("offensiveRebounds")),
                    "assists": safe_int(stats.get("assistances")),
                    "steals": safe_int(stats.get("steals")),
                    "turnovers": safe_int(stats.get("turnovers")),
                    "blocks_favour": safe_int(stats.get("blocksFavour")),
                    "blocks_against": safe_int(stats.get("blocksAgainst")),
                    "fouls_committed": safe_int(stats.get("foulsCommited")),
                    "fouls_received": safe_int(stats.get("foulsReceived")),
                    "plus_minus": safe_int(stats.get("plusMinus")),
                    "time_played": safe_int(stats.get("timePlayed"))
                }

                cur.execute("""
                    INSERT INTO team_season_stats (
                        season_code, team_code, games_played, points, valuation,
                        field_goals_2_made, field_goals_2_attempted,
                        field_goals_3_made, field_goals_3_attempted,
                        free_throws_made, free_throws_attempted,
                        field_goals_total_made, field_goals_total_attempted,
                        total_rebounds, defensive_rebounds, offensive_rebounds,
                        assists, steals, turnovers, blocks_favour, blocks_against,
                        fouls_committed, fouls_received, plus_minus, time_played
                    ) VALUES (
                        %(season_code)s, %(team_code)s, %(games_played)s, %(points)s, %(valuation)s,
                        %(field_goals_2_made)s, %(field_goals_2_attempted)s,
                        %(field_goals_3_made)s, %(field_goals_3_attempted)s,
                        %(free_throws_made)s, %(free_throws_attempted)s,
                        %(field_goals_total_made)s, %(field_goals_total_attempted)s,
                        %(total_rebounds)s, %(defensive_rebounds)s, %(offensive_rebounds)s,
                        %(assists)s, %(steals)s, %(turnovers)s, %(blocks_favour)s, %(blocks_against)s,
                        %(fouls_committed)s, %(fouls_received)s, %(plus_minus)s, %(time_played)s
                    )
                    ON CONFLICT (season_code, team_code) DO UPDATE SET
                        games_played = EXCLUDED.games_played,
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
                        time_played = EXCLUDED.time_played;
                """, values)

                total_inserted += 1

    conn.close()
    print(f"Insertion complete. Total team season stats inserted or updated: {total_inserted}")

if __name__ == "__main__":
    insert_team_season_stats()
