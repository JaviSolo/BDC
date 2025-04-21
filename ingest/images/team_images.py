import os
import requests
import psycopg2
from tqdm import tqdm
from ..config import DB_CONFIG

DEST_FOLDER = "app/static/images/teams"
os.makedirs(DEST_FOLDER, exist_ok=True)

def insert_team_logos():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("""
            SELECT team_code, crest_url
            FROM teams
            WHERE crest_url IS NOT NULL AND crest_url <> ''
        """)
        teams = cur.fetchall()

        with tqdm(total=len(teams), desc="Processing team logos") as pbar:
            for team_code, crest_url in teams:
                file_name = f"{team_code}.png"
                file_path = f"/images/teams/{file_name}"
                full_path = os.path.join(DEST_FOLDER, file_name)

                # Download file if not exists
                if not os.path.exists(full_path):
                    try:
                        response = requests.get(crest_url, timeout=10)
                        response.raise_for_status()
                        with open(full_path, "wb") as f:
                            f.write(response.content)
                        print(f"[✔] Downloaded: {team_code}")
                    except Exception as e:
                        print(f"[✘] Failed to download {team_code}: {e}")
                        pbar.update(1)
                        continue
                else:
                    print(f"[→] File already exists: {team_code}.png")

                # Insert into images_teams if not exists
                cur.execute("""
                    SELECT 1 FROM images_teams
                    WHERE team_code = %s AND context = 'crest'
                """, (team_code,))
                if not cur.fetchone():
                    cur.execute("""
                        INSERT INTO images_teams (team_code, context, file_path)
                        VALUES (%s, %s, %s)
                    """, (team_code, 'crest', file_path))
                    print(f"[+] Inserted into images_teams: {team_code}")
                else:
                    print(f"[→] Already in images_teams: {team_code}")

                pbar.update(1)

    conn.close()
    print("Finished.")

if __name__ == "__main__":
    insert_team_logos()
