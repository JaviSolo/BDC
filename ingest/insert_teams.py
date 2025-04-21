import psycopg2
import requests
from config import DB_CONFIG

#Extracting columns team_code, name, alias, is_virtual, country_code, country_name, city, 
#address, website, tickets_url, facebook_account, twitter_account, instagram_account, 
#crest_url, president, phone, fax, national_competition_code

def insert_teams():
    url = "https://api-live.euroleague.net/v3/clubs"
    response = requests.get(url)
    response.raise_for_status()

    clubs = response.json()["data"]

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    with conn.cursor() as cur:
        for club in clubs:
            team_code = club["code"]
            name = club["name"]
            alias = club["alias"]
            is_virtual = club["isVirtual"]
            country_code = club["country"]["code"] if club.get("country") else None
            country_name = club["country"]["name"] if club.get("country") else None
            city = club.get("city")
            address = club.get("address")
            website = club.get("website")
            tickets_url = club.get("ticketsURL")
            facebook_account = club.get("facebookAccount")
            twitter_account = club.get("twitterAccount")
            instagram_account = club.get("instagramAccount")
            crest_url = club["images"]["crest"] if club.get("images") else None
            president = club.get("president")
            phone = club.get("phone")
            fax = club.get("fax")
            national_competition_code = club.get("nationalCompetitionCode")

            cur.execute("""
                INSERT INTO teams (
                        team_code, name, alias, is_virtual, 
                        country_code, country_name, city, 
                        address, website, tickets_url, facebook_account, 
                        twitter_account, instagram_account, crest_url, 
                        president, phone, fax, national_competition_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (team_code) DO NOTHING;
            """, 
                (
                team_code, name, alias, is_virtual, 
                country_code, country_name, city, 
                address, website, tickets_url, facebook_account, 
                twitter_account, instagram_account, crest_url, 
                president, phone, fax, national_competition_code
            ))
    print (f"{len(clubs)} teams inserted successfully")
    conn.close()

if __name__ == "__main__":
    insert_teams()
