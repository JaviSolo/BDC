import psycopg2
import requests
import xml.etree.ElementTree as ET
from config import DB_CONFIG, COMPETITION, SEASONS
from datetime import datetime
import time

API_BASE_URL = "https://api-live.euroleague.net/v1"
HEADERS = {"Accept": "application/xml"}  # Para V1 XML

def connect_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    print("🎯 CONECTADO A:", conn.get_dsn_parameters())
    return conn

def get_player_codes_from_game(season_code_api, gamecode):
    url = f"{API_BASE_URL}/games?seasonCode={season_code_api}&gameCode={gamecode}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        print(f"[!] Error al obtener XML de game {gamecode}: {response.status_code}")
        return []
    try:
        root = ET.fromstring(response.content)
        return list({stat.find('PlayerCode').text for stat in root.findall('.//stat') if stat.find('PlayerCode') is not None and stat.find('PlayerCode').text != '0'})
    except Exception as e:
        print(f"[!] Error parseando XML del game {gamecode}: {e}")
        return []

def get_player_details_v1(player_code, season_code_api):
    url = f"{API_BASE_URL}/players?playerCode={player_code}&seasonCode={season_code_api}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        print(f"[!] Error al obtener XML del jugador {player_code}: {response.status_code}")
        return None

    if not response.content.strip():
        print(f"[!] Sin contenido para jugador {player_code}")
        return None

    try:
        root = ET.fromstring(response.content)
        return {
            'player_code': player_code,
            'full_name': root.findtext('name'),
            'birth_date': root.findtext('birthdate'),
            'nationality': root.findtext('country'),
            'height_cm': float(root.findtext('height')) * 100 if root.findtext('height') else None,
            'weight_kg': None,
            'position': root.findtext('position'),
            'image_url': None
        }
    except Exception as e:
        print(f"[!] Error al parsear XML del jugador {player_code}: {e}")
        return None

def insert_player(cur, pdata):
    query = """
        INSERT INTO players (player_code, full_name, birth_date, nationality, height_cm, weight_kg, position, image_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (player_code) DO NOTHING;
    """
    cur.execute(query, (
        pdata['player_code'],
        pdata['full_name'],
        pdata.get('birth_date'),
        pdata.get('nationality'),
        pdata.get('height_cm'),
        pdata.get('weight_kg'),
        pdata.get('position'),
        pdata.get('image_url')
    ))

def main():
    conn = connect_db()
    cur = conn.cursor()

    for season in SEASONS:
        season_api = f"{COMPETITION}{season}"
        print(f"\n📅 Temporada {season_api}")

        cur.execute("SELECT gamecode FROM games WHERE league_code = %s AND season_code = %s", (COMPETITION, str(season)))
        gamecodes_full = [row[0] for row in cur.fetchall()]
        gamecodes = [code.split("_")[-1] for code in gamecodes_full if "_" in code]

        print(f"🎮 {len(gamecodes)} partidos jugados encontrados")

        player_codes = set()
        for i, gamecode in enumerate(gamecodes, 1):
            print(f"🎯 ({i}/{len(gamecodes)}) Obteniendo jugadores del partido {gamecode}")
            player_codes.update(get_player_codes_from_game(season_api, gamecode))
            time.sleep(0.2)

        print(f"🔎 {len(player_codes)} playerCodes únicos encontrados")

        for player_code in player_codes:
            details = get_player_details_v1(player_code, season_api)
            time.sleep(0.5)
            if not details or not details.get("full_name"):
                continue

            pdata = {
                'player_code': player_code,
                'full_name': details.get("full_name"),
                'birth_date': None,
                'nationality': details.get("nationality"),
                'height_cm': int(details.get("height_cm")) if details.get("height_cm") else None,
                'weight_kg': details.get("weight_kg"),
                'position': details.get("position"),
                'image_url': details.get("image_url")
            }

            if bdate := details.get('birth_date'):
                try:
                    pdata['birth_date'] = datetime.strptime(bdate, "%d %B, %Y").date()
                except:
                    pass

            insert_player(cur, pdata)
            print(f"✅ Insertado: {pdata['full_name']} ({player_code})")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
