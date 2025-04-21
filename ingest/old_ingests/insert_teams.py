import requests
import psycopg2
from psycopg2.extras import execute_values
from config import DB_CONFIG, COMPETITION

API_BASE = "https://api-live.euroleague.net/v3"

def get_teams():
    url = f"{API_BASE}/clubs"
    resp = requests.get(url)
    resp.raise_for_status()  # Verifica si la petición ha tenido éxito
    data = resp.json()
    return data.get('data', [])

def insert_teams(teams):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            query = """
                INSERT INTO teams (
                    code, name, abbreviation, tv_code, country, city, website,
                    twitter_account, instagram_account, facebook_account,
                    venue_name, venue_capacity, venue_address, venue_images,
                    crest_url, league_code
                )
                VALUES %s
                ON CONFLICT (code) DO NOTHING;
            """
            values = [
                (
                    team['code'],
                    team['name'],
                    team.get('alias'),
                    team.get('tvCode'),
                    team['country']['name'] if team.get('country') else None,
                    team.get('city'),
                    team.get('website'),
                    team.get('twitterAccount'),
                    team.get('instagramAccount'),
                    team.get('facebookAccount'),
                    team['venue']['name'] if team.get('venue') else None,
                    team['venue']['capacity'] if team.get('venue') else None,
                    team['venue']['address'] if team.get('venue') else None,
                    ','.join(team['venue']['images'].values()) if team.get('venue') and team['venue'].get('images') else None,
                    ','.join(team['images'].values()) if team.get('images') else None,
                    COMPETITION
                ) for team in teams
            ]

            execute_values(cur, query, values)
            conn.commit()

if __name__ == "__main__":
    teams = get_teams()
    insert_teams(teams)
    print(f"✅ Insertados {len(teams)} equipos con éxito.")
