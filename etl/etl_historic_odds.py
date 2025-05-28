"""
Load MLB historical odds CSVs sitting in odds_data/ into the DB.

Assumes:
  • game table already populated (gamePk, date, home_id, away_id)
  • TEAM_INFO mapping from your utils.baseball_stats
"""
import os, glob, time, csv
import pandas as pd
from psycopg2 import connect
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from utils.baseball_stats import TEAM_INFO         # <- you already have this

load_dotenv()
DSN = os.getenv("POSTGRES_URI")
ODDS_DIR = "odds_data/"
SOURCE_NAME = "scottfree"

# ─────────── helpers ─────────────────────────────────────────
def _get_or_create_source_id(cur, src_name:str) -> int:
    cur.execute("SELECT source_id FROM odds_source WHERE src_name=%s;", (src_name,))
    row = cur.fetchone()
    if row: return row[0]
    cur.execute("INSERT INTO odds_source (src_name) VALUES (%s) RETURNING source_id;",
                (src_name,))
    return cur.fetchone()[0]

def _team_name_to_id(name:str) -> int|None:
    name = name.lower().replace(" ", "_")
    for tid, meta in TEAM_INFO.items():
        if meta["abbr"].lower() in name or meta["name"].lower().replace(" ", "_") == name:
            return tid
    return None

def _match_gamePk(cur, game_date, home_id, away_id,
                  home_runs, away_runs):
    """
    Return the single gamePk that matches
        • date
        • home / away club
        • AND the final runs for each club
    """
    cur.execute("""
        SELECT g.gamePk
          FROM game       AS g
        JOIN team_box AS th  ON th.gamePk = g.gamePk
                            AND th.is_home
                            AND th.team_id = %s
                            AND th.runs    = %s         -- home runs
        JOIN team_box AS ta  ON ta.gamePk = g.gamePk
                            AND NOT ta.is_home
                            AND ta.team_id = %s
                            AND ta.runs    = %s         -- away runs
         WHERE g.game_date = %s
        LIMIT 1;
    """, (home_id, home_runs,
          away_id, away_runs,
          game_date))
    row = cur.fetchone()
    return row[0] if row else None

# ─────────── main load routine ───────────────────────────────
def load_csv(path:str, cur, source_id:int) -> tuple[int,int]:
    df = pd.read_csv(path)
    rows, skipped = 0, 0
    payload = []
    for _, r in df.iterrows():
        home_id = _team_name_to_id(r["home_team"])
        away_id = _team_name_to_id(r["away_team"])
        if not (home_id and away_id):
            skipped += 1
            continue
        gpk = _match_gamePk(
                cur,
                r["date"],
                home_id,
                away_id,
                int(r["home_score"]),
                int(r["away_score"])
        )
        if not gpk:
            skipped += 1
            continue
        payload.append((
            gpk,
            source_id,
            int(r["away_money_line"]),
            int(r["home_money_line"]),
            float(r["away_point_spread"]),
            int(r["away_point_spread_line"]),
            float(r["home_point_spread"]),
            int(r["home_point_spread_line"]),
            float(r["over_under"]),
            int(r["over_line"]),
            int(r["under_line"]),
        ))
    if payload:
        execute_values(cur, """
            INSERT INTO game_odds (
                gamePk, source_id,
                away_money_line, home_money_line,
                away_spread,     away_spread_line,
                home_spread,     home_spread_line,
                over_under,      over_line, under_line
            ) VALUES %s
            ON CONFLICT (gamePk, source_id) DO NOTHING;
        """, payload)
        rows = len(payload)
    return rows, skipped

def main():
    csv_files = sorted(glob.glob(os.path.join(ODDS_DIR, "mlb_game_scores_*.csv")))
    if not csv_files:
        print("No CSVs found in odds_data/")
        return
    total_ins, total_skip = 0, 0
    with connect(DSN) as conn, conn.cursor() as cur:
        src_id = _get_or_create_source_id(cur, SOURCE_NAME)
        for p in csv_files:
            ins, skip = load_csv(p, cur, src_id)
            total_ins  += ins
            total_skip += skip
            conn.commit()
            print(f"✔ {os.path.basename(p)} → {ins} inserted, {skip} skipped")
    print(f"\nDone. {total_ins} rows inserted, {total_skip} skipped.")

if __name__ == "__main__":
    main()
