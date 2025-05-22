import os
import uuid
from datetime import datetime

import pandas as pd
import numpy as np
from pybaseball import schedule_and_record
import psycopg2
import statsapi
from dotenv import load_dotenv

load_dotenv()
URI = os.environ["POSTGRES_URI"]

#MLB codes
teams = [
    'ARI', 'ATL', 'BAL', 'BOS', 'CHC', 'CHW', 'CIN', 'CLE', 'COL',
    'DET', 'HOU', 'KCR', 'LAA', 'LAD', 'MIA', 'MIL', 'MIN', 'NYM',
    'NYY', 'OAK', 'PHI', 'PIT', 'SDP', 'SEA', 'SFG', 'STL', 'TBR',
    'TEX', 'TOR', 'WSN'
]

# Match To Ids in the Teams Database
team_id = pd.Series(range(40, 70), index=teams) 

#Match month Abbrev to digit
dates = {
    'Jan' : 1,
    'Feb' : 2,
    'Mar' : 3,
    'Apr' : 4,
    'May' : 5,
    'Jun' : 6,
    'Jul' : 7,
    'Aug' : 8,
    'Sep' : 9,
    'Oct' : 10,
    'Nov' : 11,
    'Dec' : 12
}

# Map internal IDs to external IDs
statsapi_map = {
    'ARI': 109, 'ATL': 144, 'BAL': 110, 'BOS': 111, 'CHC': 112, 'CHW': 145,
    'CIN': 113, 'CLE': 114, 'COL': 115, 'DET': 116, 'HOU': 117, 'KCR': 118,
    'LAA': 108, 'LAD': 119, 'MIA': 146, 'MIL': 158, 'MIN': 142, 'NYM': 121,
    'NYY': 147, 'OAK': 133, 'PHI': 143, 'PIT': 134, 'SDP': 135, 'SEA': 136,
    'SFG': 137, 'STL': 138, 'TBR': 139, 'TEX': 140, 'TOR': 141, 'WSN': 120
}

internal_to_abbrev = {v: k for k, v in team_id.to_dict().items()}
teamid_to_statsapi = {k: statsapi_map[internal_to_abbrev[k]] for k in team_id.values}

#create date time object
def convert_time(day, season=2023):
    day_month = day.split(',')[1].strip().split()
    return datetime(season, dates[day_month[0]], int(day_month[1]))


'''
Takes in a team and season and gathers all game data for that team in that season
'''
def scrape_season(season, team):
    try:
        raw_all = schedule_and_record(season, team)
        print(raw_all.head())
        raw_all['game_date'] = raw_all["Date"].apply(convert_time, args=(season,))
        home_mask = raw_all['Home_Away'] != '@'           

        raw_all['home_id']  = np.where(
            home_mask,
            raw_all['Tm'].map(team_id),
            raw_all['Opp'].map(team_id)
        )
        raw_all['away_id']  = np.where(
            home_mask,
            raw_all['Opp'].map(team_id),
            raw_all['Tm'].map(team_id)
        )

        raw_all['home_score'] = np.where(home_mask, raw_all['R'],  raw_all['RA'])
        raw_all['away_score'] = np.where(home_mask, raw_all['RA'], raw_all['R'])
        raw_all['d_n'] = np.where(raw_all["D/N"] == 'D', 1, 0)

        df = raw_all.loc[:, ["game_date", 'home_id', 'away_id', 'home_score', 'away_score', 'd_n']]
        df.insert(0, 'season', season)
        return df
    except:
        print(team)

