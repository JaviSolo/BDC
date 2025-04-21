import psycopg2
import requests
from tqdm import tqdm
from config import DB_CONFIG, SEASONS, COMPETITION

# Insert data into the games table from the V2 API
# Extracting: gamecode, season_code, competition_code, round_number, phase_type, group_name,
# date, utc_date, played, home_team_code, away_team_code, home_score, away_score,
# venue_code, attendance, local_timezone, game_number, confirmed_date, confirmed_hour,
# is_neutral_venue, game_status, winner_team_code

def insert_games():

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    with conn.cursor() as cur:
        total_inserted=0

        for season in tqdm(SEASONS, desc="Inserting games per season"):
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
                    season_code = game.get("season", {}).get("code")
                    competition_code = game.get("season", {}).get("competitionCode")
                    round_number = game.get("round")
                    phase_type = game.get("phaseType", {}).get("code")
                    group_name = game.get("group", {}).get("rawName")
                    date = game.get("date")
                    utc_date = game.get("utcDate")
                    played = game.get("played")

                    home_team_code = game.get("local", {}).get("club", {}).get("code")
                    away_team_code = game.get("road", {}).get("club", {}).get("code")
                    home_score = game.get("local", {}).get("score")
                    away_score = game.get("road", {}).get("score")

                    venue_code = game.get("venue", {}).get("code")
                    attendance = game.get("audience")
                    local_timezone = game.get("localTimeZone")

                    game_number = game.get("gameCode")
                    confirmed_date = game.get("confirmedDate")
                    confirmed_hour = game.get("confirmedHour")
                    is_neutral_venue = game.get("isNeutralVenue")
                    game_status = game.get("gameStatus")
                    winner = game.get("winner")
                    winner_team_code = winner.get("code") if isinstance(winner, dict) else None

                    cur.execute(
                        """
                        INSERT INTO games (
                            gamecode, season_code, competition_code, round_number, phase_type, group_name,
                            date, utc_date, played, home_team_code, away_team_code,
                            home_score, away_score, venue_code, attendance, local_timezone,
                            game_number, confirmed_date, confirmed_hour, is_neutral_venue,
                            game_status, winner_team_code
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (gamecode) DO UPDATE SET
                            played = EXCLUDED.played,
                            home_score = EXCLUDED.home_score,
                            away_score = EXCLUDED.away_score,
                            attendance = EXCLUDED.attendance,
                            confirmed_date = EXCLUDED.confirmed_date,
                            confirmed_hour = EXCLUDED.confirmed_hour,
                            game_status = EXCLUDED.game_status,
                            winner_team_code = EXCLUDED.winner_team_code,
                            venue_code = EXCLUDED.venue_code;
                        """,
                        (
                            gamecode, season_code, competition_code, round_number, phase_type, group_name,
                            date, utc_date, played, home_team_code, away_team_code,
                            home_score, away_score, venue_code, attendance, local_timezone,
                            game_number, confirmed_date, confirmed_hour, is_neutral_venue,
                            game_status, winner_team_code
                        )
                    )
                    total_inserted += 1

                offset += limit

    conn.close()
    print(f"Insertion complete. Total processed games: {total_inserted}")

if __name__ == "__main__":
    insert_games()
