# insert_play_by_play.py

import psycopg2
from tqdm import tqdm
import json
import pandas as pd
from euroleague_api.play_by_play_data import PlayByPlay
from config import DB_CONFIG, SEASONS

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def get_all_games():
    query = "SELECT gamecode, season_code FROM games WHERE season_code = %s"
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (f"E{SEASONS[0]}",))
            return cur.fetchall()
        
def insert_play_by_play():
    games = get_all_games()
    pbp = PlayByPlay()
    error_count = 0

    with connect_db() as conn:
        with conn.cursor() as cur:
            for gamecode, season_code in tqdm(games, desc="Inserting Play-By-Play"):
                try:
                    season_year = int(season_code[-4:])
                    game_number = int(gamecode.split("_")[-1])
                    df = pbp.get_game_play_by_play_data(season_year, game_number)

                    if df.empty:
                        continue

                    for _, row in df.iterrows():
                        play_number = row.get("NUMBEROFPLAY")
                        team_code = row.get("CODETEAM") or None
                        player_id = row.get("PLAYER_ID")
                        person_code = player_id[1:] if isinstance(player_id, str) and player_id.startswith("P") else None
                        period = row.get("PERIOD")
                        time_string = row.get("MARKERTIME")
                        event_type = row.get("PLAYTYPE") or "Unknown"
                        description = row.get("PLAYINFO")
                        points_a = row.get("POINTS_A")
                        points_b = row.get("POINTS_B")

                        # Normalizar valores NaN
                        points_a = None if pd.isna(points_a) else points_a
                        points_b = None if pd.isna(points_b) else points_b

                        insert_query = """
                            INSERT INTO play_by_play (
                                gamecode, play_number, team_code, person_code, period,
                                time_string, event_type, description, points_a, points_b, season_code
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                            points_a,
                            points_b,
                            season_code
                        ))

                    conn.commit()

                except Exception:
                    error_count += 1
                    conn.rollback()

    print(f"\n✅ Play-by-play ingestion completed. Total errors: {error_count}")

if __name__ == "__main__":
    insert_play_by_play()
