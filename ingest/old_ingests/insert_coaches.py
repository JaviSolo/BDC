import psycopg2
import requests
from config import DB_CONFIG, COMPETITION
from datetime import datetime

API_BASE = "https://api-live.euroleague.net/v3"
COACHES_TEST_SEASON = "2023"  # Cambia esto para probar otra temporada

def connect_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    return conn

def get_coaches_for_season(cursor, season_code):
    cursor.execute("""
        SELECT gamecode FROM games
        WHERE season_code = %s AND played = TRUE;
    """, (season_code,))
    rows = cursor.fetchall()
    gamecodes = [row[0] for row in rows]

    coaches = set()
    for gamecode in gamecodes:
        print(f"📎 Consultando partido {gamecode}")
        url = f"{API_BASE}/competitions/{COMPETITION}/seasons/{season_code}/games/{gamecode}/stats"
        try:
            resp = requests.get(url)
            if resp.status_code == 200:
                data = resp.json()
                for side in ["local", "road"]:
                    coach_data = data.get(side, {}).get("coach")
                    team_data = data.get(side, {}).get("team")
                    if coach_data and team_data:
                        coach_code = coach_data.get("code")
                        team_code = team_data.get("code")
                        if coach_code and team_code:
                            coaches.add((coach_code, team_code))
            else:
                print(f"❌ No se pudo obtener el partido {gamecode}: {resp.status_code}")
        except Exception as e:
            print(f"❌ Error en partido {gamecode}: {e}")
    return list(coaches)

def get_coach_info(season_code, coach_code):
    url = f"{API_BASE}/competitions/{COMPETITION}/seasons/{season_code}/coaches/{coach_code}"
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json().get("coach", {})
        return {
            "coach_code": data.get("code"),
            "full_name": data.get("name"),
            "birth_date": data.get("birthDate", "")[:10],
            "nationality": data.get("country", {}).get("name"),
            "height_cm": data.get("height"),
            "weight_kg": data.get("weight"),
            "twitter_account": data.get("twitterAccount"),
            "instagram_account": data.get("instagramAccount"),
            "facebook_account": data.get("facebookAccount"),
            "image_url": list(data.get("images", {}).values())[0] if data.get("images") else None,
        }
    except Exception as e:
        print(f"❌ Error obteniendo info de coach {coach_code}: {e}")
        return None

def insert_coach(cursor, coach_data):
    cursor.execute("SELECT coach_id FROM coaches WHERE full_name = %s", (coach_data["full_name"],))
    if cursor.fetchone():
        print(f"↪️ Coach ya existe: {coach_data['full_name']}")
        return

    cursor.execute("""
        INSERT INTO coaches (
            full_name, birth_date, nationality,
            height_cm, weight_kg,
            twitter_account, instagram_account, facebook_account,
            image_url, league_code
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'E')
    """, (
        coach_data["full_name"],
        coach_data["birth_date"] or None,
        coach_data["nationality"],
        coach_data["height_cm"],
        coach_data["weight_kg"],
        coach_data["twitter_account"],
        coach_data["instagram_account"],
        coach_data["facebook_account"],
        coach_data["image_url"]
    ))
    print(f"✅ Insertado coach: {coach_data['full_name']}")

def main():
    conn = connect_db()
    cursor = conn.cursor()

    season_code = COACHES_TEST_SEASON
    print(f"\n📅 Procesando temporada {season_code}")
    coach_pairs = get_coaches_for_season(cursor, season_code)

    for coach_code, _ in coach_pairs:
        print(f"👤 Obteniendo info del coach {coach_code}")
        coach_info = get_coach_info(season_code, coach_code)
        if coach_info:
            insert_coach(cursor, coach_info)

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
