import psycopg2
import requests
from config import DB_CONFIG, COMPETITION


#Extracting columns season_code', 'competition_code', 'start_year', 'name', alias, start_date, end_date, winner_team_code

def insert_seasons():
    url = f"https://api-live.euroleague.net/v2/competitions/{COMPETITION}/seasons"
    response = requests.get(url)
    response.raise_for_status()

    seasons = response.json()["data"]

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    with conn.cursor() as cur:
        for season in seasons:
            season_code = season["code"]
            competition_code = season["competitionCode"]
            start_year = season["year"]
            name = season["name"]
            alias = season["alias"]
            start_date = season["startDate"]
            end_date = season["endDate"]
            winner_team_code = None
            if season.get("winner"):
                winner_team_code = season["winner"].get("code")


            cur.execute("""
                INSERT INTO SEASONS (season_code, competition_code, start_year, name, alias, start_date, end_date, winner_team_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (season_code) DO NOTHING;
            """, 
                (season_code, competition_code, start_year, name, alias, start_date, end_date, winner_team_code
            ))
    print (f"{len(seasons)} seasons inserted successfully")
    conn.close()

if __name__ == "__main__":
    insert_seasons()