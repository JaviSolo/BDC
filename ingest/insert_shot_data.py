# insert_shot_data.py

import psycopg2
from tqdm import tqdm
import pandas as pd
from euroleague_api.shot_data import ShotData
from config import DB_CONFIG, SEASONS

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def get_all_games():
    query = "SELECT gamecode, season_code FROM games WHERE season_code = %s"
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (f"E{SEASONS[0]}",))
            return cur.fetchall()

def insert_shot_data():
    games = get_all_games()
    shot_data = ShotData()
    error_count = 0

    with connect_db() as conn:
        with conn.cursor() as cur:
            for gamecode, season_code in tqdm(games, desc="Inserting Shot Data"):
                try:
                    season_year = int(season_code[-4:])
                    game_number = int(gamecode.split("_")[-1])
                    df = shot_data.get_game_shot_data(season_year, game_number)

                    if df.empty:
                        continue

                    for _, row in df.iterrows():
                        play_number = row.get("NUM_ANOT")
                        team_code = row.get("TEAM") or None
                        player_id = row.get("ID_PLAYER")
                        person_code = player_id[1:] if isinstance(player_id, str) and player_id.startswith("P") else None
                        period = row.get("MINUTE")
                        time_string = row.get("CONSOLE")
                        event_type = row.get("ID_ACTION")
                        description = row.get("ACTION")
                        points_scored = row.get("POINTS")

                        # Coordenadas y zona
                        coord_x = row.get("COORD_X")
                        coord_y = row.get("COORD_Y")
                        zone = row.get("ZONE")

                        # Booleans
                        fastbreak = bool(row.get("FASTBREAK"))
                        second_chance = bool(row.get("SECOND_CHANCE"))
                        points_off_turnover = bool(row.get("POINTS_OFF_TURNOVER"))

                        points_a = None if pd.isna(row.get("POINTS_A")) else row.get("POINTS_A")
                        points_b = None if pd.isna(row.get("POINTS_B")) else row.get("POINTS_B")
                        timestamp_utc = row.get("UTC")

                        insert_query = """
                            INSERT INTO shot_data (
                                gamecode, play_number, team_code, person_code, period,
                                time_string, event_type, description, season_code,
                                points_scored, points_a, points_b,
                                coord_x, coord_y, zone,
                                fastbreak, second_chance, points_off_turnover,
                                timestamp_utc
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                        """

                        cur.execute(insert_query, (
                            gamecode,
                            play_number,
                            team_code,
                            person_code,
                            period,
                            time_string,
                            event_type,
                            description,
                            season_code,
                            points_scored,
                            points_a,
                            points_b,
                            coord_x,
                            coord_y,
                            zone,
                            fastbreak,
                            second_chance,
                            points_off_turnover,
                            timestamp_utc
                        ))

                    conn.commit()

                except Exception:
                    error_count += 1
                    conn.rollback()

    print(f"\n✅ Shot data ingestion completed. Total errors: {error_count}")

if __name__ == "__main__":
    insert_shot_data()
