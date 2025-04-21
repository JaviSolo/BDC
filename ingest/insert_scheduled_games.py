# insert_scheduled_games.py

import requests
import psycopg2
import xml.etree.ElementTree as ET
from tqdm import tqdm
from config import DB_CONFIG, SEASONS, COMPETITION

# ----------------------
# Database connection
# ----------------------
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# ----------------------
# Parse single <item>
# ----------------------
def parse_item(item, cursor):
    def get_text(tag):
        el = item.find(tag)
        return el.text.strip() if el is not None and el.text else None

    gamecode = get_text('gamecode')
    # Check if the game exists and has a score in the 'games' table
    cursor.execute("""
        SELECT home_score, away_score
        FROM games
        WHERE gamecode = %s
    """, (gamecode,))
    result = cursor.fetchone()
    played = result is not None and result[0] is not None and result[1] is not None

    return {
        'gamecode': gamecode,
        'game_number': int(get_text('game')) if get_text('game') else None,
        'season_code': gamecode.split('_')[0],
        'round_number': int(get_text('gameday')) if get_text('gameday') else None,
        'round_code': get_text('round'),
        'round_name': get_text('group'),
        'home_team_code': get_text('homecode'),
        'away_team_code': get_text('awaycode'),
        'date': get_text('date'),
        'hour': get_text('startime'),
        'end_hour': get_text('endtime'),
        'venue_code': get_text('arenacode'),
        'venue_name': get_text('arenaname'),
        'venue_capacity': int(get_text('arenacapacity')) if get_text('arenacapacity') else None,
        'confirmed_date': get_text('confirmeddate') == 'true',
        'confirmed_hour': get_text('confirmedtime') == 'true',
        'played': played
    }

# ----------------------
# Insert venue if missing
# ----------------------
def ensure_venue_exists(cursor, match):
    cursor.execute("SELECT 1 FROM venues WHERE venue_code = %s", (match['venue_code'],))
    if cursor.fetchone() is None:
        cursor.execute("""
            INSERT INTO venues (venue_code, name, capacity)
            VALUES (%s, %s, %s)
            ON CONFLICT (venue_code) DO NOTHING
        """, (match['venue_code'], match['venue_name'], match['venue_capacity']))

# ----------------------
# Insert match into DB
# ----------------------
def insert_match(cursor, match):
    ensure_venue_exists(cursor, match)
    cursor.execute("""
        INSERT INTO scheduled_games (
            gamecode, game_number, season_code, round_number, round_code, round_name,
            home_team_code, away_team_code, date, hour, end_hour,
            venue_code, venue_name, venue_capacity,
            confirmed_date, confirmed_hour, played
        ) VALUES (
            %(gamecode)s, %(game_number)s, %(season_code)s, %(round_number)s, %(round_code)s, %(round_name)s,
            %(home_team_code)s, %(away_team_code)s, %(date)s, %(hour)s, %(end_hour)s,
            %(venue_code)s, %(venue_name)s, %(venue_capacity)s,
            %(confirmed_date)s, %(confirmed_hour)s, %(played)s
        )
        ON CONFLICT (gamecode) DO UPDATE SET
            game_number = EXCLUDED.game_number,
            season_code = EXCLUDED.season_code,
            round_number = EXCLUDED.round_number,
            round_code = EXCLUDED.round_code,
            round_name = EXCLUDED.round_name,
            home_team_code = EXCLUDED.home_team_code,
            away_team_code = EXCLUDED.away_team_code,
            date = EXCLUDED.date,
            hour = EXCLUDED.hour,
            end_hour = EXCLUDED.end_hour,
            venue_code = EXCLUDED.venue_code,
            venue_name = EXCLUDED.venue_name,
            venue_capacity = EXCLUDED.venue_capacity,
            confirmed_date = EXCLUDED.confirmed_date,
            confirmed_hour = EXCLUDED.confirmed_hour,
            played = EXCLUDED.played;
    """, match)

# ----------------------
# Main ingestion logic
# ----------------------
def main():
    with get_connection() as conn:
        with conn.cursor() as cur:
            for season_code in tqdm(SEASONS, desc="Inserting scheduled games"):
                url = f"https://api-live.euroleague.net/v1/schedules?seasonCode={COMPETITION}{season_code}"
                try:
                    response = requests.get(url)
                    response.raise_for_status()
                    root = ET.fromstring(response.content)
                    for item in root.findall("item"):
                        match = parse_item(item, cur)
                        insert_match(cur, match)
                    conn.commit()
                except Exception as e:
                    print(f"Error processing season {season_code}: {e}")
                    conn.rollback()

if __name__ == "__main__":
    main()
