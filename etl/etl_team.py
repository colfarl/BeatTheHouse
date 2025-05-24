from utils.baseball_stats import * 
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

load_dotenv()
DSN: str = os.environ["POSTGRES_URI"]


def insert_teams(conn, teams):
    query = """
        INSERT INTO team (team_id, season_year, name, abbr, league, division)
        VALUES %s
        ON CONFLICT (team_id, season_year) DO NOTHING;
    """
    with conn.cursor() as cur:
        execute_values(cur, query, teams)
    conn.commit()

if __name__ == "__main__":
    conn = psycopg2.connect(DSN)
    all_team_rows = []
    for year in range(2013, 2026):
        for team_id, meta in TEAM_INFO.items():
            row = [team_id, year, meta['name'], meta['abbr'], meta['league'], meta['division']]
            all_team_rows.append(row)
    insert_teams(conn, all_team_rows)
    conn.close()