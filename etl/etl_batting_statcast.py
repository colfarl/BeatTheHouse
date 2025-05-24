import os
import pandas as pd
from psycopg2 import connect
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()
DSN = os.getenv("POSTGRES_URI")

batting_dir = "batting_data"
years = list(range(2015, 2026))

columns_to_use = [
    'player_id', 'year', 'player_age', 'ab', 'pa', 'hit', 'single', 'double', 'triple', 'home_run',
    'strikeout', 'walk', 'k_percent', 'bb_percent', 'batting_avg', 'slg_percent', 'on_base_percent',
    'on_base_plus_slg', 'isolated_power', 'babip', 'b_rbi', 'xba', 'xslg', 'woba', 'xwoba', 'xobp',
    'xiso', 'exit_velocity_avg', 'sweet_spot_percent', 'barrel_batted_rate', 'hard_hit_percent',
    'avg_best_speed', 'meatball_swing_percent', 'iz_contact_percent', 'whiff_percent', 'swing_percent'
]

insert_query = f"""
    INSERT INTO savant_batter_stats ({", ".join(columns_to_use)})
    VALUES %s
    ON CONFLICT (player_id, year) DO NOTHING;
"""

conn = connect(DSN)
cur = conn.cursor()

for year in years:
    path = os.path.join(batting_dir, f"batting{year}.csv")
    if not os.path.exists(path):
        print(f"Missing: {path}")
        continue

    df = pd.read_csv(path)
    df['year'] = year
    if not all(col in df.columns for col in columns_to_use):
        print(f"Skipping year {year}: columns missing")
        continue

    
    df = df[columns_to_use].dropna(subset=["player_id"])
    records = [tuple(x) for x in df.itertuples(index=False, name=None)]
    if records:
        execute_values(cur, insert_query, records)
        conn.commit()
        print(f"Inserted {len(records)} rows for {year}")

cur.close()
conn.close()