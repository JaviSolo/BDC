from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'options': '-c client_encoding=UTF8'
}

# Detect current season automatically based on current date
current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month

# Euroliga va de octubre a junio, así que si estamos antes de julio, sigue la temporada anterior
season_year = current_year - 1 if current_month < 7 else current_year
SEASONS = [season_year]
#SEASONS = list(range(2000, 2030))

COMPETITION = "E"
