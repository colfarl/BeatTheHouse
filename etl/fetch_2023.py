import os
import uuid
from datetime import datetime

import pandas as pd
import numpy as np
from pybaseball import schedule_and_record
import psycopg2
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

team_id = pd.Series(range(40, 70), index=teams) 

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

def convert_time(day, season=2023):
    day_month = day.split(',')[1].strip().split()
    return datetime(season, dates[day_month[0]], int(day_month[1]))

season = 2023
frames = []
for team in teams:
    try:
        raw_all = schedule_and_record(season, team)

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
        frames.append(df)
    except:
        print("ERROR: " + team)

df = pd.concat(frames, ignore_index=True)
df = df.drop_duplicates()

print(df.head())
print(len(df))