"""
Incrementally load MLB games that have finished since the last date
already stored in the `game` table.

Run daily from cron / systemd-timer / GitHub Actions etc.
"""
import os, time, datetime as dt
from contextlib import closing
from psycopg2 import connect
from dotenv import load_dotenv
import statsapi                    # pip install MLB-StatsAPI

# ────────  your own helpers  ────────
from utils.baseball_stats import *

load_dotenv()
DSN = os.getenv("POSTGRES_URI")
SLEEP_BETWEEN_GAMES = 0.20            # polite pause (seconds) to avoid hammering the API

# ────────────────────────────────────────────────────────────────────────────
def _get_latest_game_date(cur) -> dt.date | None:
    """Return the max(game_date) already present, or None if table empty."""
    cur.execute("SELECT MAX(game_date) FROM game;")
    row = cur.fetchone()
    return row[0]          # either a date object or None

def _collect_new_game_pks(start_date: dt.date, end_date: dt.date) -> list[int]:
    """Call Stats API schedule and return Pks of games that have *finished*."""
    new_game_pks: list[int] = []
    one_day = dt.timedelta(days=1)
    d = start_date
    while d <= end_date:
        for g in safe_get_games_in_range(d.isoformat(), d.isoformat()):  # wrapper helper
            if g["status"].startswith(("Final", "Completed")):
                new_game_pks.append(g["game_id"])
        d += one_day
    # de-dupe just in case (double-headers sometimes appear twice)
    return sorted(set(new_game_pks))

# ────────────────────────────────────────────────────────────────────────────
def main() -> None:
    today = dt.date.today()

    with closing(connect(DSN)) as conn, conn.cursor() as cur:
        last_date = _get_latest_game_date(cur)
        if last_date is None:
            print("⚠️  No rows in `game` table — run the full-load ETL first.")
            return

        start_date = last_date + dt.timedelta(days=1)
        if start_date > today:
            print("✅ Database already up-to-date.")
            return

        game_pks = _collect_new_game_pks(start_date, today)
        if not game_pks:
            print("✅ No completed games to load.")
            return

        print(f"🔄 Loading {len(game_pks)} new finished games "
              f"({start_date} → {today}) …")

        player_cache: set[int] = set()     # avoids repeated people look-ups

        for idx, gamePk in enumerate(game_pks, 1):
            try:
                box      = safe_boxscore_data(gamePk)
                raw_box  = safe_boxscore_raw(gamePk)
                season   = int(box["gameId"].split('/')[0])

                process_team_fielding(raw_box, gamePk, cur)
                process_team_box(box, gamePk, cur)
                process_fielders(raw_box, gamePk, cur)
                process_pitchers(box, gamePk, cur)
                process_batters(box, gamePk, cur)
                process_player_teams(box, gamePk, season, cur)
                process_players(box, gamePk, cur, player_cache)

                conn.commit()
                print(f"   • committed {idx}/{len(game_pks)}  (gamePk {gamePk})")
                time.sleep(SLEEP_BETWEEN_GAMES)

            except Exception as e:
                conn.rollback()
                print(f"💥  gamePk {gamePk} failed → rolled back.  {e}")

        print("✅ incremental update complete.")

# ────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
