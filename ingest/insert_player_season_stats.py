import psycopg2
import requests
from tqdm import tqdm
from config import DB_CONFIG, SEASONS, COMPETITION

def insert_player_season_stats():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    with conn.cursor() as cur:
        total_inserts = 0

        for season in tqdm(SEASONS, desc="Inserting player season stats"):
            season_code = f"{COMPETITION}{season}"

            # ⚠️ Solo jugadores que jugaron en esta temporada
            cur.execute("""
                SELECT DISTINCT person_code 
                FROM player_game_stats 
                WHERE gamecode LIKE %s
            """, (f"{season_code}%",))
            players = [row[0] for row in cur.fetchall()]

            for person_code in tqdm(players, leave=False, desc=f"Season {season_code}"):
                url = f"https://api-live.euroleague.net/v2/competitions/{COMPETITION}/seasons/{season_code}/people/{person_code}/stats"
                headers = {"Accept": "application/json"}

                try:
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                    data = response.json()
                except requests.RequestException as e:
                    print(f"Failed to fetch stats for {person_code} in {season_code}: {e}")
                    continue

                games = data.get("games", [])
                if not games:
                    continue

                stats_by_phase = {}
                for game in games:
                    phase_type = game["game"]["phaseType"]["code"]
                    stats = game.get("stats", {})
                    team_code = game.get("playerClubCode")

                    if phase_type not in stats_by_phase:
                        stats_by_phase[phase_type] = {
                            "team_code": team_code,
                            "games_played": 0,
                            "games_started": 0,
                            "minutes_played": 0,
                            "points": 0,
                            "pir": 0,
                            "field_goals_2_made": 0,
                            "field_goals_2_attempted": 0,
                            "field_goals_3_made": 0,
                            "field_goals_3_attempted": 0,
                            "free_throws_made": 0,
                            "free_throws_attempted": 0,
                            "total_rebounds": 0,
                            "offensive_rebounds": 0,
                            "defensive_rebounds": 0,
                            "assists": 0,
                            "steals": 0,
                            "turnovers": 0,
                            "blocks": 0,
                            "blocks_against": 0,
                            "fouls_committed": 0,
                            "fouls_drawn": 0,
                            "plus_minus": 0,
                            "wins": None,
                            "losses": None,
                            "double_doubles": None,
                            "triple_doubles": None
                        }

                    p = stats_by_phase[phase_type]
                    def s(key):
                        val = stats.get(key)
                        return int(val) if val is not None else 0

                    p["games_played"] += 1
                    p["games_started"] += int(stats.get("startFive", False))
                    p["minutes_played"] += s("timePlayed")
                    p["points"] += s("points")
                    p["pir"] += s("valuation")
                    p["field_goals_2_made"] += s("fieldGoalsMade2")
                    p["field_goals_2_attempted"] += s("fieldGoalsAttempted2")
                    p["field_goals_3_made"] += s("fieldGoalsMade3")
                    p["field_goals_3_attempted"] += s("fieldGoalsAttempted3")
                    p["free_throws_made"] += s("freeThrowsMade")
                    p["free_throws_attempted"] += s("freeThrowsAttempted")
                    p["total_rebounds"] += s("totalRebounds")
                    p["offensive_rebounds"] += s("offensiveRebounds")
                    p["defensive_rebounds"] += s("defensiveRebounds")
                    p["assists"] += s("assistances")
                    p["steals"] += s("steals")
                    p["turnovers"] += s("turnovers")
                    p["blocks"] += s("blocksFavour")
                    p["blocks_against"] += s("blocksAgainst")
                    p["fouls_committed"] += s("foulsCommited")
                    p["fouls_drawn"] += s("foulsReceived")
                    p["plus_minus"] += s("plusMinus")

                for phase_type, stats in stats_by_phase.items():
                    values = {
                        "season_code": season_code,
                        "person_code": person_code,
                        "phase_type": phase_type,
                        **stats
                    }

                    cur.execute("""
                        INSERT INTO player_season_stats (
                            season_code, person_code, phase_type, team_code,
                            games_played, games_started, minutes_played, points, pir,
                            field_goals_2_made, field_goals_2_attempted,
                            field_goals_3_made, field_goals_3_attempted,
                            free_throws_made, free_throws_attempted,
                            total_rebounds, offensive_rebounds, defensive_rebounds,
                            assists, steals, turnovers, blocks, blocks_against,
                            fouls_committed, fouls_drawn, plus_minus,
                            wins, losses, double_doubles, triple_doubles
                        ) VALUES (
                            %(season_code)s, %(person_code)s, %(phase_type)s, %(team_code)s,
                            %(games_played)s, %(games_started)s, %(minutes_played)s, %(points)s, %(pir)s,
                            %(field_goals_2_made)s, %(field_goals_2_attempted)s,
                            %(field_goals_3_made)s, %(field_goals_3_attempted)s,
                            %(free_throws_made)s, %(free_throws_attempted)s,
                            %(total_rebounds)s, %(offensive_rebounds)s, %(defensive_rebounds)s,
                            %(assists)s, %(steals)s, %(turnovers)s, %(blocks)s, %(blocks_against)s,
                            %(fouls_committed)s, %(fouls_drawn)s, %(plus_minus)s,
                            %(wins)s, %(losses)s, %(double_doubles)s, %(triple_doubles)s
                        )
                        ON CONFLICT (season_code, person_code, phase_type)
                        DO UPDATE SET
                            team_code = EXCLUDED.team_code,
                            games_played = EXCLUDED.games_played,
                            games_started = EXCLUDED.games_started,
                            minutes_played = EXCLUDED.minutes_played,
                            points = EXCLUDED.points,
                            pir = EXCLUDED.pir,
                            field_goals_2_made = EXCLUDED.field_goals_2_made,
                            field_goals_2_attempted = EXCLUDED.field_goals_2_attempted,
                            field_goals_3_made = EXCLUDED.field_goals_3_made,
                            field_goals_3_attempted = EXCLUDED.field_goals_3_attempted,
                            free_throws_made = EXCLUDED.free_throws_made,
                            free_throws_attempted = EXCLUDED.free_throws_attempted,
                            total_rebounds = EXCLUDED.total_rebounds,
                            offensive_rebounds = EXCLUDED.offensive_rebounds,
                            defensive_rebounds = EXCLUDED.defensive_rebounds,
                            assists = EXCLUDED.assists,
                            steals = EXCLUDED.steals,
                            turnovers = EXCLUDED.turnovers,
                            blocks = EXCLUDED.blocks,
                            blocks_against = EXCLUDED.blocks_against,
                            fouls_committed = EXCLUDED.fouls_committed,
                            fouls_drawn = EXCLUDED.fouls_drawn,
                            plus_minus = EXCLUDED.plus_minus,
                            wins = EXCLUDED.wins,
                            losses = EXCLUDED.losses,
                            double_doubles = EXCLUDED.double_doubles,
                            triple_doubles = EXCLUDED.triple_doubles
                    """, values)

                    total_inserts += 1

    conn.close()
    print(f"Insertion complete. Total player season stats inserted or updated: {total_inserts}")

if __name__ == "__main__":
    insert_player_season_stats()
