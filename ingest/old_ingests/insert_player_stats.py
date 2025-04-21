import psycopg2
import requests
from config import DB_CONFIG, COMPETITION, SEASONS
import time

API_BASE_URL = "https://api-live.euroleague.net/v3"
HEADERS = {"Accept": "application/json"}

PHASES = ["RS", "PO", "FF"]  # Fases posibles

STAT_TYPES = ["traditional", "advanced"]
LIMIT = 1000
SLEEP = 0.1


def connect_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    print("🎯 CONECTADO A:", conn.get_dsn_parameters())
    return conn


def get_total_players(stat_type, season_code, phase_code):
    url = f"{API_BASE_URL}/competitions/{COMPETITION}/statistics/players/{stat_type}"
    params = {
        "SeasonCode": season_code,
        "phaseTypeCode": phase_code,
        "offset": 0,
        "limit": 1
    }
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code != 200:
        print(f"[!] Error al obtener total de {stat_type}: {response.status_code}")
        return 0
    return response.json().get("total", 0)


def get_stats(stat_type, season_code, phase_code, offset=0, limit=LIMIT):
    url = f"{API_BASE_URL}/competitions/{COMPETITION}/statistics/players/{stat_type}"
    params = {
        "SeasonCode": season_code,
        "phaseTypeCode": phase_code,
        "offset": offset,
        "limit": limit
    }
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code != 200:
        print(f"[!] Error al obtener {stat_type} stats: {response.status_code}")
        return []
    return response.json().get("players", [])


def insert_stat(cur, pdata):
    query = """
        INSERT INTO player_stats (
            player_id, team_id, game_id, stat_type, minutes_played, points, rebounds, assists,
            steals, blocks, turnovers, field_goals_made, field_goals_attempted,
            three_points_made, three_points_attempted, free_throws_made, free_throws_attempted,
            effective_fg_pct, true_shooting_pct, offensive_rebound_pct, defensive_rebound_pct,
            assist_ratio, turnover_ratio, usage_rate, gamecode, league_code
        )
        VALUES (%(player_id)s, %(team_id)s, NULL, %(stat_type)s, %(minutes_played)s, %(points)s, %(rebounds)s, %(assists)s,
                %(steals)s, %(blocks)s, %(turnovers)s, %(fg_made)s, %(fg_attempted)s,
                %(three_made)s, %(three_attempted)s, %(ft_made)s, %(ft_attempted)s,
                %(efg_pct)s, %(ts_pct)s, %(oreb_pct)s, %(dreb_pct)s, %(ast_ratio)s, %(to_ratio)s, %(usage_rate)s,
                NULL, %(league_code)s)
        ON CONFLICT (player_id, gamecode) DO NOTHING;
    """
    cur.execute(query, pdata)


def get_player_id(cur, player_code):
    cur.execute("SELECT player_id FROM players WHERE player_code = %s", (player_code,))
    res = cur.fetchone()
    return res[0] if res else None


def get_team_id(cur, team_code):
    cur.execute("SELECT team_id FROM teams WHERE code = %s", (team_code,))
    res = cur.fetchone()
    return res[0] if res else None


def main():
    conn = connect_db()
    cur = conn.cursor()

    for stat_type in STAT_TYPES:
        print(f"\n📊 Ingestando stats tipo: {stat_type.upper()}")
        for season in SEASONS:
            season_code = f"{COMPETITION}{season}"
            for phase in PHASES:
                print(f"\n📅 Temporada {season_code}, Fase {phase}")
                total = get_total_players(stat_type, season_code, phase)
                print(f"🔢 Total jugadores esperados: {total}")

                for offset in range(0, total, LIMIT):
                    stats = get_stats(stat_type, season_code, phase, offset)
                    print(f"🚚 Procesando {len(stats)} jugadores (offset {offset})")

                    for player in stats:
                        code = player["player"]["code"]
                        name = player["player"]["name"]
                        player_id = get_player_id(cur, code)
                        team_code = player["player"]["team"]["code"].split(";")[0]
                        team_id = get_team_id(cur, team_code)

                        if not player_id or not team_id:
                            print(f"[!] Jugador o equipo no encontrado: {code}, {team_code}")
                            continue

                        pdata = {
                            "player_id": player_id,
                            "team_id": team_id,
                            "stat_type": stat_type,
                            "minutes_played": round(player.get("minutesPlayed", 0), 2),
                            "points": int(player.get("pointsScored", 0)),
                            "rebounds": int(player.get("totalRebounds", 0)),
                            "assists": int(player.get("assists", 0)),
                            "steals": int(player.get("steals", 0)),
                            "blocks": int(player.get("blocks", 0)),
                            "turnovers": int(player.get("turnovers", 0)),
                            "fg_made": int(player.get("twoPointersMade", 0)) + int(player.get("threePointersMade", 0)),
                            "fg_attempted": int(player.get("twoPointersAttempted", 0)) + int(player.get("threePointersAttempted", 0)),
                            "three_made": int(player.get("threePointersMade", 0)),
                            "three_attempted": int(player.get("threePointersAttempted", 0)),
                            "ft_made": int(player.get("freeThrowsMade", 0)),
                            "ft_attempted": int(player.get("freeThrowsAttempted", 0)),
                            "efg_pct": float(player.get("effectiveFieldGoalPercentage", "0").replace('%','') or 0),
                            "ts_pct": float(player.get("trueShootingPercentage", "0").replace('%','') or 0),
                            "oreb_pct": float(player.get("offensiveReboundsPercentage", "0").replace('%','') or 0),
                            "dreb_pct": float(player.get("defensiveReboundsPercentage", "0").replace('%','') or 0),
                            "ast_ratio": float(player.get("assistsRatio", "0").replace('%','') or 0),
                            "to_ratio": float(player.get("turnoversRatio", "0").replace('%','') or 0),
                            "usage_rate": float(player.get("freeThrowsRate", "0").replace('%','') or 0),
                            "league_code": COMPETITION,
                            "gamecode": None
                        }

                        insert_stat(cur, pdata)
                        print(f"✅ {name} ({code}) - {stat_type} [{season_code} {phase}]")
                        time.sleep(SLEEP)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
