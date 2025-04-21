import psycopg2
import requests
from config import DB_CONFIG

#Extracting columns 'name' and 'competition_code'

def insert_competitions():
    url ="https://api-live.euroleague.net/v2/competitions"
    response = requests.get(url)
    response.raise_for_status()

    competitions = response.json()["data"]

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    with conn.cursor() as cur:
        for competition in competitions:
            competition_code = competition["code"]
            name = competition["name"]

            cur.execute("""
                INSERT INTO competitions (competition_code, name)
                Values (%s, %s)
                ON CONFLICT (competition_code) DO NOTHING;
            """, (competition_code, name))

    print (f"{len(competitions)} competitions inserted successfully")
    conn.close()

if __name__ == "__main__":
    insert_competitions()