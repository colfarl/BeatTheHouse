from utils.baseball_stats import * 
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import time
import requests

load_dotenv()
DSN: str = os.environ["POSTGRES_URI"]


def insert_games(conn, games):
    query = """
        INSERT INTO game (
            gamePk, season_year, game_date, venue,
            home_team_id, away_team_id, first_pitch_ts,
            attendance, weather_temp_f, wind_mph,
            home_score, away_score
        )
        VALUES %s
        ON CONFLICT (gamePk) DO NOTHING;
    """
    with conn.cursor() as cur:
        execute_values(cur, query, games)
    conn.commit()


if __name__ == "__main__":
    all_games = []
    conn = psycopg2.connect(DSN)
    for year in range(2025, 2026):
        schedule = safe_get_schedule(year)
        for game in schedule:
            if game['status'] != "Final":
                continue
            gamePk = game['game_id']
            home_id = game['home_id']
            away_id = game['away_id']
            if home_id not in TEAM_INFO or away_id not in TEAM_INFO:
                continue
            box = safe_boxscore_data(gamePk)
            if box is None:
                print(f"skipping game: {gamePk}")
                continue
            meta = box["gameBoxInfo"]
            meta_parsed = extract_meta_data(meta)
            game_date = game['game_date']
            venue = game["venue_name"]
            
            first_pitch_ts = game['game_datetime']
            attendance = meta_parsed['attendance']
            weather_temp_f = meta_parsed['weather_temp_f']
            wind_mph = meta_parsed["wind_mph"]
            home_score = game['home_score']
            away_score = game['away_score']
            insert_row = [ gamePk, year, game_date, venue, home_id, away_id, first_pitch_ts, attendance, weather_temp_f, wind_mph, home_score, away_score]
            all_games.append(insert_row)

            if len(all_games) % 200 == 0:
                print(f'game {len(all_games)} reached') 
            time.sleep(1)

        print(f"Finished {year}, inserting {len(all_games)} games...")
        insert_games(conn, all_games)
        all_games = []
        time.sleep(10)
    conn.close()