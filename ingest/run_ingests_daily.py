import subprocess
import os
from datetime import datetime

# Ruta explícita al Python del venv (importante para cron)
PYTHON_EXEC = "/home/bdc-admin/bdc-backend/venv/bin/python"

# Paths
INGEST_DIR = os.path.join(os.path.dirname(__file__))
LOGS_DIR = os.path.join(INGEST_DIR, "..", "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

SCRIPTS = [
    "insert_coach_teams.py",
    "insert_competitions.py",
    "insert_game_referees.py",
    "insert_games.py",
    "insert_people.py",
    "insert_play_by_play.py",
    "insert_player_game_stats.py",
    "insert_player_season_stats.py",
    "insert_player_teams.py",
    "insert_scheduled_games.py",
    "insert_seasons.py",
    "insert_shot_data.py",
    "insert_standings.py",
    "insert_team_game_stats.py",
    "insert_team_info.py",
    "insert_team_season_stats.py",
    "insert_team_venues.py",
    "insert_teams.py",
    "insert_venues.py"
]

today = datetime.now().strftime("%Y-%m-%d")
log_file = os.path.join(LOGS_DIR, f"ingest_{today}.log")

with open(log_file, "w") as log:
    log.write(f"🔄 Ingestion started at {datetime.now()}\n\n")

    for script in SCRIPTS:
        script_path = os.path.join(INGEST_DIR, script)
        with open(log_file, "a") as log:
            log.write(f"▶ Running {script}...\n")
            try:
                result = subprocess.run(
                    [PYTHON_EXEC, script_path],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                log.write(result.stdout)
                if result.stderr:
                    log.write("⚠️ STDERR:\n" + result.stderr)
            except Exception as e:
                log.write(f"❌ Failed to run {script}: {e}\n")
            log.write("\n" + "="*80 + "\n\n")

    with open(log_file, "a") as log:
        log.write(f"\n✅ Ingestion completed at {datetime.now()}\n")
