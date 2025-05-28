import os
import pandas as pd
from psycopg2 import connect
from dotenv import load_dotenv
from utils.baseball_stats import * 
from contextlib import closing

if __name__ == "__main__":
    load_dotenv()
    DSN = os.getenv("POSTGRES_URI")

    game_pks = get_all_game_pks(DSN)          # all gamePk’s already in `game`
    player_cache: set[int] = set()            # avoid re-hitting /people for known players

    BATCH_SIZE  = 250                         # commit after this many games
    SLEEP_SEC   = 0.20                        # polite pause between calls

    with closing(connect(DSN)) as conn, conn.cursor() as cur:
        for idx, gamePk in enumerate(game_pks, 1):
            try:
                # ─── fetch wrapper & raw JSON ───────────────────────────────
                box_score = safe_boxscore_data(gamePk)
                raw_box   = safe_boxscore_raw(gamePk)
                if not (box_score and raw_box):
                    print(f"⚠️  skipped {gamePk}: could not fetch boxscore")
                    continue

                season_year = int(box_score["gameId"].split('/')[0])

                # ─── load dimension & fact tables ──────────────────────────
                process_team_fielding(raw_box,        gamePk, cur)
                process_team_box(box_score,           gamePk, cur)
                process_fielders(raw_box,             gamePk, cur)
                process_pitchers(box_score,           gamePk, cur)
                process_batters(box_score,            gamePk, cur)
                process_player_teams(box_score, gamePk, season_year, cur)
                process_players(box_score,      gamePk, cur, player_cache)

            except Exception as e:
                conn.rollback()                       # keep DB clean
                print(f"💥  {gamePk} failed – rolled back. ({e})")
                continue

            # ─── periodic commit & progress ping ───────────────────────────
            if idx % BATCH_SIZE == 0:
                conn.commit()
                print(f"✅ committed {idx}/{len(game_pks)} games …")

            time.sleep(SLEEP_SEC)                     # API-friendly pacing

        conn.commit()                                 # final commit
        print("🏁 ETL run complete.")
