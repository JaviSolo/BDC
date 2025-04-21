import psycopg2
import pandas as pd
from euroleague_api.EuroLeagueData import EuroLeagueData
from config import DB_CONFIG, SEASONS, COMPETITION
from datetime import datetime

def connect_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    print("🎯 CONECTADO A:", conn.get_dsn_parameters())
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM games;")
        total = cur.fetchone()[0]
        print(f"📦 Total actual en tabla GAMES: {total}")
    return conn

def get_team_lookup(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT code, team_id, venue_name FROM teams WHERE league_code = %s;", (COMPETITION,))
        rows = cur.fetchall()
        code_to_id = {}
        code_to_venue = {}
        for code, team_id, venue in rows:
            code_to_id[code] = team_id
            code_to_venue[code] = venue
        return code_to_id, code_to_venue

def insert_game(cur, row, season, code_to_id, code_to_venue):
    try:
        gamecode = row['gamecode']
        print(f"➕ Intentando insertar: {gamecode}")

        # Validar y obtener los equipos
        home_code = row['homecode']
        away_code = row['awaycode']
        home_team_id = code_to_id.get(home_code)
        away_team_id = code_to_id.get(away_code)

        # Verificar que los equipos existan
        if home_team_id is None or away_team_id is None:
            print(f"[!] Equipo no encontrado: {home_code} vs {away_code}")
            return 0

        # Validar puntajes
        try:
            home_score = int(row['homescore']) if row['homescore'] is not None else 0
            away_score = int(row['awayscore']) if row['awayscore'] is not None else 0
        except ValueError:
            print(f"[!] Puntajes inválidos para el juego {gamecode} -> home_score: {row['homescore']}, away_score: {row['awayscore']}")
            return 0

        # Convertir fecha
        try:
            date_obj = datetime.strptime(row['date'], "%b %d, %Y").date()
        except ValueError:
            print(f"[!] Fecha inválida para el juego {gamecode}: {row['date']}")
            return 0

        venue = code_to_venue.get(home_code, "")
        # Asegurarse que no haya caracteres invisibles o espacios en blanco adicionales en venue
        venue = venue.strip() if venue else ""

        # Asegurar que los valores booleanos sean correctamente convertidos a 1 o 0
        played = bool(row['played']) if 'played' in row else False
        local_timezone = 1  # Si no tienes un valor, puedes asignar un valor por defecto
        confirmed_date = bool(row.get('date'))
        confirmed_hour = bool(row.get('time'))
        # Aseguramos que `attendance` sea 0 si no está disponible
        attendance = row['attendance'] if 'attendance' in row else 0

        # Verificación de los datos antes de insertar
        print(f"✅ Valores a insertar para {gamecode}: {home_team_id}, {away_team_id}, {home_score}, {away_score}, {date_obj}, {venue}")

        # Depuración adicional: muestra los tipos y valores exactos
        print(f"gamecode: {gamecode} (Tipo: {type(gamecode)})")
        print(f"season_code: {season} (Tipo: {type(season)})")
        print(f"round_number: {row['gameday']} (Tipo: {type(row['gameday'])})")
        print(f"date: {date_obj} (Tipo: {type(date_obj)})")
        print(f"home_team_id: {home_team_id} (Tipo: {type(home_team_id)})")
        print(f"away_team_id: {away_team_id} (Tipo: {type(away_team_id)})")
        print(f"home_score: {home_score} (Tipo: {type(home_score)})")
        print(f"away_score: {away_score} (Tipo: {type(away_score)})")
        print(f"venue: {venue} (Tipo: {type(venue)})")
        print(f"attendance: {attendance} (Tipo: {type(attendance)})")
        print(f"league_code: {COMPETITION} (Tipo: {type(COMPETITION)})")
        print(f"played: {played} (Tipo: {type(played)})")
        print(f"phase_type: {row['round']} (Tipo: {type(row['round'])})")
        print(f"group_name: {row['group']} (Tipo: {type(row['group'])})")
        print(f"local_timezone: {local_timezone} (Tipo: {type(local_timezone)})")
        print(f"confirmed_date: {confirmed_date} (Tipo: {type(confirmed_date)})")
        print(f"confirmed_hour: {confirmed_hour} (Tipo: {type(confirmed_hour)})")

        # Asegurémonos de que los valores sean del tipo correcto
        values_to_insert = (
            gamecode, str(season), row['gameday'], date_obj,
            home_team_id, away_team_id,
            home_score, away_score,
            venue if venue else '',  # Reemplazar None o valores vacíos con ''
            attendance,  # Usamos 0 si no hay asistencia
            COMPETITION,
            played, row['round'], row['group'],
            local_timezone, confirmed_date, confirmed_hour
        )

        # Imprimir los valores antes de la inserción
        print(f"Valores a insertar: {values_to_insert}")
        print(f"Número de valores a insertar: {len(values_to_insert)}")

        # Insertar el juego en la base de datos usando gamecode
        cur.execute("""
            INSERT INTO games (
                gamecode, season_code, round_number, date,
                home_team_id, away_team_id, home_score, away_score,
                venue, attendance, league_code, played, phase_type,
                group_name, local_timezone, confirmed_date, confirmed_hour
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (gamecode) DO NOTHING;
        """, values_to_insert)

        if cur.rowcount > 0:
            return 1
        else:
            print(f"[·] No insertado (ya existe en base de datos): {gamecode}")
            return 0

    except Exception as e:
        print(f"[💥] Error al insertar {row.get('gamecode')}: {e}")
        return 0


def main():
    conn = connect_db()
    print("🧪 Conectado a:", conn.get_dsn_parameters())
    euro = EuroLeagueData()
    code_to_id, code_to_venue = get_team_lookup(conn)

    resumen = []

    for season in SEASONS:
        print(f"\n🔄 Procesando temporada {season}...")
        try:
            df = euro.get_game_metadata_season(season)

            if not isinstance(df, pd.DataFrame):
                print(f"[!] La función devolvió algo inesperado: {type(df)}")
                resumen.append((season, "INVALID", 0))
                continue

            if df.empty:
                print(f"[!] No hay partidos para la temporada {season}")
                resumen.append((season, "VACÍA", 0))
                continue

            dupes = df[df.duplicated(subset='gamecode', keep=False)]
            if not dupes.empty:
                print(f"[‼️] {len(dupes)} duplicados encontrados en el DataFrame:")
                print(dupes[['gamecode', 'homecode', 'awaycode']])

            insertados = 0
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    insertados += insert_game(cur, row, season, code_to_id, code_to_venue)

            print(f"✅ Temporada {season}: {insertados} partidos insertados")
            resumen.append((season, "OK", insertados))

        except Exception as e:
            print(f"[X] Error inesperado en la temporada {season}: {e}")
            resumen.append((season, "ERROR", 0))

    conn.close()

    print("\n📊 RESUMEN FINAL:")
    for season, estado, total in resumen:
        print(f" - {season}: {estado} ({total} partidos)")

    print("\n🏁 Inserción finalizada.")

if __name__ == "__main__":
    main()
