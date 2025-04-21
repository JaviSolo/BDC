import psycopg2
import requests
from config import DB_CONFIG, SEASONS, COMPETITION
from time import sleep

# Extracting team_code, venue_code, season_code
def get_team_venue_assignments():
    assignments = []

    for season_year in SEASONS:
        season_code = f"{COMPETITION}{season_year}"
        url = f"https://api-live.euroleague.net/v2/competitions/{COMPETITION}/seasons/{season_code}/venues"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            print(f"[{season_code}] Total records: {len(data)}")

            for record in data:
                team_code = record.get("clubCode")
                venues = record.get("venues", [])

                for venue in venues:
                    venue_code = venue.get("code")
                    if team_code and venue_code:
                        assignments.append({
                            "team_code": team_code,
                            "venue_code": venue_code,
                            "season_code": season_code
                        })
                    else:
                        print(f"Saltado: team_code={team_code}, venue_code={venue_code}")

        except requests.RequestException as e:
            print(f"Error fetching venues for season {season_code}: {e}")
        sleep(0.2)

    print(f"Total assignments collected: {len(assignments)}")
    return assignments


# Extracting is_primary column
def add_is_primary_field(assignments):
    url = "https://api-live.euroleague.net/v2/clubs"
    try:
        response = requests.get(url)
        response.raise_for_status()
        clubs = response.json().get("data", [])  # ✅ corregido para acceder a la lista real
    except requests.RequestException as e:
        print(f"Error fetching clubs: {e}")
        return assignments
    
    backup_map = {}
    for club in clubs:
        team_code = club.get("code")
        backup_venue = club.get("venueBackup")
        if team_code and isinstance(backup_venue, dict):  # ✅ asegurarse que no es None
            backup_map[team_code] = backup_venue.get("code")

    for item in assignments:
        team = item["team_code"]
        venue = item["venue_code"]
        is_backup = backup_map.get(team)
        item["is_primary"] = venue != is_backup  # True si no es backup

    return assignments

# Insert objects in team_venues table
def insert_team_venues():
    assignments = get_team_venue_assignments()
    assignments = add_is_primary_field(assignments)

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    with conn.cursor() as cur:
        for item in assignments:
            cur.execute(
                """
                INSERT INTO team_venues (
                    team_code, venue_code, season_code, is_primary
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (team_code, venue_code, season_code) DO NOTHING;
                """,
                (
                    item["team_code"],
                    item["venue_code"],
                    item["season_code"],
                    item["is_primary"]
                )
            )

    print(f"{len(assignments)} team_venue assignments inserted successfully.")

if __name__ == "__main__":
    insert_team_venues()
