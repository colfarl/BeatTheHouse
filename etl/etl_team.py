from utils.baseball_stats import * 
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

load_dotenv()
DSN: str = os.environ["POSTGRES_URI"]


TEAM_INFO = {
    108: {"abbr": "LAA", "name": "Los Angeles Angels", "league": "AL", "division": "W"},
    109: {"abbr": "AZ",  "name": "Arizona Diamondbacks", "league": "NL", "division": "W"},
    110: {"abbr": "BAL", "name": "Baltimore Orioles", "league": "AL", "division": "E"},
    111: {"abbr": "BOS", "name": "Boston Red Sox", "league": "AL", "division": "E"},
    112: {"abbr": "CHC", "name": "Chicago Cubs", "league": "NL", "division": "C"},
    113: {"abbr": "CIN", "name": "Cincinnati Reds", "league": "NL", "division": "C"},
    114: {"abbr": "CLE", "name": "Cleveland Guardians", "league": "AL", "division": "C"},
    115: {"abbr": "COL", "name": "Colorado Rockies", "league": "NL", "division": "W"},
    116: {"abbr": "DET", "name": "Detroit Tigers", "league": "AL", "division": "C"},
    117: {"abbr": "HOU", "name": "Houston Astros", "league": "AL", "division": "W"},
    118: {"abbr": "KC",  "name": "Kansas City Royals", "league": "AL", "division": "C"},
    119: {"abbr": "LAD", "name": "Los Angeles Dodgers", "league": "NL", "division": "W"},
    120: {"abbr": "WSH", "name": "Washington Nationals", "league": "NL", "division": "E"},
    121: {"abbr": "NYM", "name": "New York Mets", "league": "NL", "division": "E"},
    133: {"abbr": "ATH", "name": "Oakland Athletics", "league": "AL", "division": "W"},
    134: {"abbr": "PIT", "name": "Pittsburgh Pirates", "league": "NL", "division": "C"},
    135: {"abbr": "SD",  "name": "San Diego Padres", "league": "NL", "division": "W"},
    136: {"abbr": "SEA", "name": "Seattle Mariners", "league": "AL", "division": "W"},
    137: {"abbr": "SF",  "name": "San Francisco Giants", "league": "NL", "division": "W"},
    138: {"abbr": "STL", "name": "St. Louis Cardinals", "league": "NL", "division": "C"},
    139: {"abbr": "TB",  "name": "Tampa Bay Rays", "league": "AL", "division": "E"},
    140: {"abbr": "TEX", "name": "Texas Rangers", "league": "AL", "division": "W"},
    141: {"abbr": "TOR", "name": "Toronto Blue Jays", "league": "AL", "division": "E"},
    142: {"abbr": "MIN", "name": "Minnesota Twins", "league": "AL", "division": "C"},
    143: {"abbr": "PHI", "name": "Philadelphia Phillies", "league": "NL", "division": "E"},
    144: {"abbr": "ATL", "name": "Atlanta Braves", "league": "NL", "division": "E"},
    145: {"abbr": "CWS", "name": "Chicago White Sox", "league": "AL", "division": "C"},
    146: {"abbr": "MIA", "name": "Miami Marlins", "league": "NL", "division": "E"},
    147: {"abbr": "NYY", "name": "New York Yankees", "league": "AL", "division": "E"},
    158: {"abbr": "MIL", "name": "Milwaukee Brewers", "league": "NL", "division": "C"},
}


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