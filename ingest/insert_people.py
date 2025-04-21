import psycopg2
import requests
from config import DB_CONFIG
from tqdm import tqdm

def insert_people():
    BASE_URL = "https://api-live.euroleague.net/v2/people"
    LIMIT = 500
    OFFSET = 0

    # Paso 1: Obtener el número total de personas
    total = 0
    try:
        res = requests.get(f"{BASE_URL}?limit=1", headers={"Accept": "application/json"})
        res.raise_for_status()
        total = res.json().get("total", 0)
    except Exception as e:
        print(f"Error fetching total count: {e}")
        return

    print(f"Total people to insert: {total}")

    # Paso 2: Conectar a la BBDD
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    with conn.cursor() as cur, tqdm(total=total, desc="Inserting People") as pbar:
        while OFFSET < total:
            try:
                response = requests.get(f"{BASE_URL}?limit={LIMIT}&offset={OFFSET}", headers={"Accept": "application/json"})
                response.raise_for_status()
                people = response.json().get("data", [])

                for person in people:
                    person_code = person.get("code")
                    name = person.get("name")
                    alias = person.get("alias")
                    passport_name = person.get("passportName")
                    passport_surname = person.get("passportSurname")
                    jersey_name = person.get("jerseyName")
                    abbreviated_name = person.get("abbreviatedName")

                    country = person.get("country") or {}
                    country_code = country.get("code")
                    country_name = country.get("name")

                    height = person.get("height")
                    weight = person.get("weight")

                    birth_date_raw = person.get("birthDate")
                    birth_date = birth_date_raw.split("T")[0] if birth_date_raw else None

                    birth_country = person.get("birthCountry") or {}
                    birth_country_code = birth_country.get("code")
                    birth_country_name = birth_country.get("name")

                    twitter_account = person.get("twitterAccount")
                    instagram_account = person.get("instagramAccount")
                    facebook_account = person.get("facebookAccount")
                    is_referee = person.get("isReferee")
                    image_url = person.get("images", {}).get("medium")

                    cur.execute(
                        """
                        INSERT INTO people (
                            person_code, name, alias, passport_name, passport_surname,
                            jersey_name, abbreviated_name, country_code, country_name,
                            height, weight, birth_date, birth_country_code, birth_country_name,
                            twitter_account, instagram_account, facebook_account, is_referee, image_url
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (person_code) DO NOTHING;
                        """,
                        (
                            person_code, name, alias, passport_name, passport_surname,
                            jersey_name, abbreviated_name, country_code, country_name,
                            height, weight, birth_date, birth_country_code, birth_country_name,
                            twitter_account, instagram_account, facebook_account, is_referee, image_url
                        )
                    )
                    pbar.update(1)

                OFFSET += LIMIT

            except Exception as e:
                print(f"Error at offset {OFFSET}: {e}")
                break

    conn.close()
    print("Insertion completed.")

if __name__ == "__main__":
    insert_people()
