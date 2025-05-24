import os
import pandas as pd
from psycopg2 import connect
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()
DSN = os.getenv("POSTGRES_URI")

pitching_years = list(range(2019, 2026))
columns = [
    'player_id', 'year', 'player_age', 'p_game', 'p_formatted_ip', 'pa', 'ab', 'hit',
    'single', 'double', 'triple', 'home_run', 'strikeout', 'walk', 'k_percent', 'bb_percent',
    'batting_avg', 'slg_percent', 'p_earned_run', 'p_blown_save', 'p_out', 'p_win', 'p_loss',
    'p_balk', 'p_era', 'p_opp_on_base_avg', 'p_total_stolen_base', 'p_pickoff_attempt_1b',
    'p_pickoff_attempt_2b', 'p_pickoff_attempt_3b', 'p_pickoff_1b', 'p_pickoff_2b',
    'p_pickoff_3b', 'p_pickoff_error_1b', 'p_pickoff_error_2b', 'p_pickoff_error_3b',
    'xba', 'xslg', 'woba', 'xwoba', 'xobp', 'xiso', 'meatball_percent', 'pitch_count',
    'in_zone_percent', 'whiff_percent', 'f_strike_percent', 'groundballs_percent',
    'flyballs_percent', 'popups_percent', 'n', 'arm_angle', 'n_fastball_formatted',
    'fastball_avg_speed', 'fastball_avg_spin', 'n_breaking_formatted', 'breaking_avg_speed',
    'breaking_avg_spin', 'n_offspeed_formatted', 'offspeed_avg_speed', 'offspeed_avg_spin'
]

INSERT_SQL = f"""
    INSERT INTO savant_pitcher_stats ({", ".join(columns)})
    VALUES %s
    ON CONFLICT (player_id, year) DO NOTHING;
"""

pitching_dir = "pitching_data"

def main():
    conn = connect(DSN)
    cur = conn.cursor()

    for year in pitching_years:
        path = os.path.join(pitching_dir, f"pitching{year}.csv")
        if not os.path.exists(path):
            print(f"Missing: {path}")
            continue

        df = pd.read_csv(path)
        df['year'] = year
        if not all(col in df.columns for col in columns):
            print(f"Skipping {year}: missing columns")
            continue

        df = df[columns].dropna(subset=["player_id"])
        for col in columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        records = [tuple(row) for row in df.itertuples(index=False, name=None)]
        if records:
            try:
                execute_values(cur, INSERT_SQL, records)
            except Exception as e:
                print("🚨 Error during insert:", e)
                print("Sample row:", records[0])
                raise
            conn.commit()
            print(f"Inserted {len(records)} records for {year}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
