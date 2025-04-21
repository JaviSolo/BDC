import psycopg2
import requests
from config import DB_CONFIG, SEASONS, COMPETITION

API_BASE = "https://api-live.euroleague.net/v3"

def connect_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    return conn

def get_gamecodes_for_season(cursor, season_code):
    cursor.execute("""
        SELECT gamecode FROM games
        WHERE season_code = %s AND played = TRUE;
    """, (season_code,))
    return [row[0] for row in cursor.fetchall()]

def get_game_id_from_api(season_code, gamecode):
    url = f"{API_BASE}/competitions/{COMPETITION}/seasons/{season_code}/games/{gamecode}/metadata"
    try:
        resp = requests.get(url)
        if resp.status_code == 404:
            raise ValueError("404 Not Found")
        resp.raise_for_status()
        data = resp.json()
        return data.get("game", {}).get("id")
    except Exception as e:
        raise RuntimeError(f"❌ Error obteniendo gameId para {gamecode}: {e}")

def insert_game_id(cursor, gamecode, game_id):
    cursor.execute("""
        INSERT INTO game_ids (gamecode, game_id)
        VALUES (%s, %s)
        ON CONFLICT (gamecode) DO NOTHING;
    """, (gamecode, game_id))

def season_supported(cursor, season_code):
    gamecodes = get_gamecodes_for_season(cursor, season_code)
    if not gamecodes:
        print(f"⏭️  Temporada {season_code} sin partidos jugados.")
        return False
    test_gamecode = gamecodes[0]
    try:
        get_game_id_from_api(season_code, test_gamecode)
        return True
    except:
        print(f"❌ API V3 no soporta la temporada {season_code}. Saltando.")
        return False

def main():
    conn = connect_db()
    cursor = conn.cursor()

    for season_code in map(str, SEASONS):
        print(f"\n📅 Procesando temporada {season_code}")
        if not season_supported(cursor, season_code):
            continue

        gamecodes = get_gamecodes_for_season(cursor, season_code)
        for gamecode in gamecodes:
            print(f"📎 Consultando {gamecode}")
            try:
                game_id = get_game_id_from_api(season_code, gamecode)
                if game_id:
                    insert_game_id(cursor, gamecode, game_id)
                    print(f"✅ Insertado: {gamecode} → {game_id}")
                else:
                    print(f"⚠️  gameId no encontrado en respuesta: {gamecode}")
            except Exception as e:
                print(e)

    cursor.close()
    conn.close()
    print("\n🏁 Proceso terminado.")

if __name__ == "__main__":
    main()
