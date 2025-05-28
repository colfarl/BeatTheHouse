import statsapi
import re
import requests
from contextlib import closing
from typing import Iterable, List, Optional, Tuple, TypedDict, Dict, Any
from psycopg2 import connect
from psycopg2.extras import execute_values
import time

#TEAM SPECIFIC DATA FROM 2013 - PRESENT 
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

# stats api function to get all games for a given year 
def get_schedule(year: int):
    return statsapi.schedule(start_date=f"{year}-03-01", end_date=f"{year}-11-30")

# stats api function to get data for a specific team
def get_team(id):
    res = statsapi.lookup_team(id)
    if len(res) == 0:
        print(id)
    return res

# stats api function to retrieve box score of a game
def safe_boxscore_raw(gamePk, max_retries=5, delay=4):
    for attempt in range(max_retries):
        try:
            url = f"https://statsapi.mlb.com/api/v1/game/{gamePk}/boxscore"
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Retrying gamePk {gamePk} due to error: {e}")
            time.sleep(delay * (attempt + 1))  # exponential backoff
    print(f"Failed to retrieve gamePk {gamePk} after {max_retries} attempts")
    return None
    

#Extract miscellaneous data from box_score
def extract_meta_data(info_list):
    def find_value(label):
        return next((item["value"] for item in info_list if item["label"] == label), None)

    # Raw values from info list
    raw_weather = find_value("Weather")           # e.g. "59 degrees, Cloudy."
    raw_wind = find_value("Wind")                 # e.g. "11 mph, In From LF."
    raw_attendance = find_value("Att")            # e.g. "4,256."
    raw_first_pitch = find_value("First pitch")   # e.g. "1:06 PM."

    # Processed values
    weather_temp_f = int(raw_weather.split()[0]) if raw_weather else None
    wind_mph = int(raw_wind.split()[0]) if raw_wind else None
    if raw_attendance:
        digits_only = re.sub(r"[^\d]", "", raw_attendance)
        attendance = int(digits_only) if digits_only else None
    else:
        attendance = None
    first_pitch_str = raw_first_pitch if raw_first_pitch else None

    return {
        "weather_temp_f": weather_temp_f,
        "wind_mph": wind_mph,
        "attendance": attendance,
        "first_pitch_str": first_pitch_str  # if needed for parsing manually
    }

#### FUNCTIONS TO ATTEMPT SAFE RETRIES ON API 
def safe_boxscore_data(gamePk, max_retries=5, delay=2):
    for attempt in range(max_retries):
        try:
            return statsapi.boxscore_data(gamePk)
        except requests.exceptions.RequestException as e:
            print(f"Retrying gamePk {gamePk} due to error: {e}")
            time.sleep(delay * (attempt + 1))  # exponential backoff
    print(f"Failed to retrieve gamePk {gamePk} after {max_retries} attempts")
    return None

def safe_get_schedule(year, retries=3, delay=5):
    for attempt in range(retries):
        try:
            return statsapi.schedule(
                start_date=f"{year}-03-01",
                end_date=f"{year}-11-30"
            )
        except requests.exceptions.RequestException as e:
            print(f"Retry {attempt+1} for year {year} due to error: {e}")
            time.sleep(delay * (attempt + 1))  # Exponential backoff
    print(f"Failed to retrieve schedule for {year}")
    return []


def get_all_game_pks(dsn: str) -> List[int]:
    """
    Return every gamePk stored in the `game` table.

    Parameters
    ----------
    dsn : str
        PostgreSQL connection string.

    Returns
    -------
    list[int]
        Sorted list of gamePk integers.
    """
    with closing(connect(dsn)) as conn, conn.cursor() as cur:
        cur.execute("SELECT gamePk FROM game ORDER BY gamePk;")
        return [row[0] for row in cur.fetchall()]

from typing import Dict, List, Tuple, Any, Optional

# type alias for readability
BatRow = Tuple[int, int, int, int, str,
               int, int, int, int, int, int,
               int, int, int, int, int, int]

def _parse_batting_order(order_str: Optional[str]) -> Optional[int]:
    """
    '601'  -> 6   (most games)
    '601.1' in rare double-switch lines -> 6 as well
    None   -> None
    """
    if not order_str:
        return None
    return int(order_str.split('.')[0]) // 100

def map_player_batting(gamePk: int,
                       team_id: int,
                       players: Dict[str, Any],
                       batter_ids: List[int]) -> List[BatRow]:
    """
    Parameters
    ----------
    gamePk, team_id : current game / club
    players         : boxscore['teams'][side]['players']  (dict keyed 'ID{pid}')
    batter_ids      : boxscore['teams'][side]['batters']  (list[int])

    Returns
    -------
    list[tuple]  ready for execute_values into player_batting
    """
    rows: List[BatRow] = []

    for pid in batter_ids:
        pkey = f"ID{pid}"
        p = players[pkey]

        b_stats = p["stats"]["batting"]
        # Some spring-training games have empty stat lines
        if not b_stats:
            continue

        rows.append((
            gamePk,
            pid,
            team_id,
            _parse_batting_order(p.get("battingOrder")),
            p["position"]["abbreviation"],

            b_stats.get("atBats",      0),
            b_stats.get("runs",        0),
            b_stats.get("hits",        0),
            b_stats.get("doubles",     0),
            b_stats.get("triples",     0),
            b_stats.get("homeRuns",    0),
            b_stats.get("rbi",         0),
            b_stats.get("baseOnBalls", 0),
            b_stats.get("strikeOuts",  0),
            b_stats.get("stolenBases", 0),
            b_stats.get("hitByPitch",  0),
            b_stats.get("leftOnBase",  0)
        ))

    return rows

def process_batters(box, gamePk, cur):
    for side in ("away", "home"):
        team_id     = box[side]["team"]["id"]
        players     = box[side]["players"]
        batter_ids  = box[side]["batters"]

        bat_rows = map_player_batting(gamePk, team_id, players, batter_ids)
        if not bat_rows:
            continue

        # sanity peek
        #print(bat_rows[0])

        execute_values(cur, """
            INSERT INTO player_batting (
                gamePk, player_id, team_id, batting_order, position,
                ab, r, h, "2b", "3b", hr,
                rbi, bb, so, sb, hbp, lob
            ) VALUES %s
            ON CONFLICT (gamePk, player_id) DO NOTHING;
        """, bat_rows)

# ─────────────────────────────────────
#  P L A Y E R   P I T C H I N G
# ─────────────────────────────────────

PitchRow = Tuple[int, int, int, bool,      # gamePk, player_id, team_id, is_starting
                 int, int, int, int,       # outs_recorded, bf, h, r
                 int, int, int, int,       # er, bb, so, hr
                 int, int,                # pitches, strikes
                 bool, bool, bool]         # win_flag, save_flag, hold_flag


def _innings_to_outs(ip_str: str) -> int:
    """
    '5.2' → 17  (5 full innings * 3 + 2 outs)
    '1.0' →  3
    '0.0' →  0
    """
    if not ip_str or ip_str == '0.0':
        return 0
    whole, frac = ip_str.split('.')
    return int(whole) * 3 + int(frac)


def _parse_decision(note: Optional[str], letter: str) -> bool:
    """
    note examples: '(W, 1-0)', '(S, 2)', '(H, 5)'
    Returns True if the leading letter matches (`W`, `S`, `H`)
    """
    return bool(note and note.startswith(f'({letter}'))


def map_player_pitching(gamePk: int,
                        team_id: int,
                        players: Dict[str, Any],
                        pitcher_ids: List[int]) -> List[PitchRow]:

    rows: List[PitchRow] = []
    if not pitcher_ids:          # rare games with no pitching stats (forfeits)
        return rows

    starter_id = pitcher_ids[0]  # first listed is always the starter

    for pid in pitcher_ids:
        p = players[f"ID{pid}"]
        p_stats = p["stats"]["pitching"]
        if not p_stats:          # should never happen, but guard anyway
            continue

        note = p_stats.get("note")
        bf = p_stats.get("battersFaced")
        if bf is None:
            bf = (p_stats.get("atBats",0) +
                p_stats.get("baseOnBalls",0) +
                p_stats.get("hitByPitch",0) +
                p_stats.get("sacBunts",0) +
                p_stats.get("sacFlies",0))
            
        rows.append((
            gamePk,
            pid,
            team_id,
            pid == starter_id,                         # is_starting
            _innings_to_outs(p_stats.get("inningsPitched", "0.0")),
            bf,          # BF
            p_stats.get("hits",            0),
            p_stats.get("runs",            0),
            p_stats.get("earnedRuns",      0),
            p_stats.get("baseOnBalls",     0),         # BB
            p_stats.get("strikeOuts",      0),
            p_stats.get("homeRuns",        0),
            p_stats.get("pitchesThrown",   # ← statsapi renamed this in 2024;
                     p_stats.get("numberOfPitches", 0)),
            p_stats.get("strikes",         0),
            _parse_decision(note, 'W'),                # win_flag
            _parse_decision(note, 'S'),                # save_flag
            _parse_decision(note, 'H')                 # hold_flag
        ))

    return rows


def process_pitchers(box: Dict[str, Any], gamePk: int, cur) -> None:
    """
    Insert rows into player_pitching for both teams of the current game.
    """
    for side in ("away", "home"):
        team_id      = box[side]["team"]["id"]
        players      = box[side]["players"]
        pitcher_ids  = box[side]["pitchers"]           # appearance order list

        pitch_rows = map_player_pitching(
            gamePk, team_id, players, pitcher_ids)

        if not pitch_rows:
            continue

        # quick eyeball check
        #print("PITCH", pitch_rows[0])

        execute_values(cur, """
            INSERT INTO player_pitching (
                gamePk, player_id, team_id, is_starting,
                outs_recorded, bf, h, r, er, bb, so,
                hr, pitches, strikes, win_flag, save_flag, hold_flag
            ) VALUES %s
            ON CONFLICT (gamePk, player_id) DO NOTHING;
        """, pitch_rows)

# ───────────────────────────────────────────
#  P L A Y E R   F I E L D I N G   (RAW JSON)
# ───────────────────────────────────────────
from typing import List, Tuple, Dict, Any

FieldRow = Tuple[int, int, int, str, int, int, int, int]
#            gamePk, pid, team,  pos,  PO,  A,  E,  DP


def _player_fielding_line(p_json: Dict[str, Any]) -> Dict[str, int]:
    """
    Return the *game* fielding stat line for one player.
    If the wrapper stripped fielding, this will be an empty dict.
    """
    return p_json.get("stats", {}).get("fielding", {})


def map_player_fielding_raw(gamePk: int,
                            team_id: int,
                            players: Dict[str, Any]) -> List[FieldRow]:
    """
    Parameters
    ----------
    players : data["teams"][side]["players"]  (dict keyed 'ID{pid}')
    """
    rows: List[FieldRow] = []

    for pkey, p in players.items():
        f_stats = _player_fielding_line(p)

        # ignore players who never touched the ball defensively
        if not f_stats or (
            f_stats.get("putOuts", 0) +
            f_stats.get("assists", 0) +
            f_stats.get("errors", 0) +
            f_stats.get("doublePlays", 0)
        ) == 0:
            continue

        rows.append((
            gamePk,
            p["person"]["id"],
            team_id,
            p["position"]["abbreviation"],           # 'SS', 'C', 'P', …
            f_stats.get("putOuts",     0),
            f_stats.get("assists",     0),
            f_stats.get("errors",      0),
            f_stats.get("doublePlays", 0)
        ))

    return rows


def process_fielders(box: Dict[str, Any], gamePk: int, cur) -> None:
    """
    Insert player_fielding rows for both clubs using the *raw* JSON.
    """
    for side in ("away", "home"):
        team_id = box["teams"][side]["team"]["id"]
        players = box["teams"][side]["players"]

        fld_rows = map_player_fielding_raw(gamePk, team_id, players)
        if not fld_rows:
            continue

        # sanity peek
        #print("FLD", fld_rows[0])

        
        execute_values(cur, """
            INSERT INTO player_fielding (
                gamePk, player_id, team_id, position,
                putouts, assists, errors, double_plays
            ) VALUES %s
            ON CONFLICT (gamePk, player_id) DO NOTHING;
        """, fld_rows)

# ───────────────────────────────────────────
#  T E A M   B O X   (wrapper JSON)
# ───────────────────────────────────────────
from typing import List, Tuple, Dict, Any


TeamRow = Tuple[int, int, bool,   # gamePk, team_id, is_home
                int, int, int, int, int, int,   # runs, hits, 2B, 3B, HR, RBI
                int, int, int, int, int,        # BB, SO, SB, LOB, outs_ip
                float, int, int]                # ERA, pitches, strikes


def _str_innings_to_outs(ip_str: str | None) -> int:
    """
    '9.0' → 27,  '5.2' → 17,  None/'0.0' → 0
    """
    if not ip_str or ip_str == '0.0':
        return 0
    whole, frac = ip_str.split(".")
    return int(whole) * 3 + int(frac)


def _get_era(pitch_stats: Dict[str, Any]) -> float | None:
    era_str = pitch_stats.get("era")
    if era_str in (None, "", "-.--"):
        return None
    try:
        return float(era_str)
    except ValueError:
        return None


def map_team_box(gamePk: int,
                 side: str,
                 team_block: Dict[str, Any]) -> TeamRow:
    """
    `team_block` is box['teams'][side]
    """
    batting = team_block["teamStats"]["batting"]
    pitching = team_block["teamStats"]["pitching"]

    # StatsAPI renamed `numberOfPitches` → `pitchesThrown` (2024)
    pitches = pitching.get("pitchesThrown",
             pitching.get("numberOfPitches", 0))

    return (
        gamePk,
        team_block["team"]["id"],
        side == "home",

        batting.get("runs",        0),
        batting.get("hits",        0),
        batting.get("doubles",     0),
        batting.get("triples",     0),
        batting.get("homeRuns",    0),
        batting.get("rbi",         0),

        batting.get("baseOnBalls", 0),
        batting.get("strikeOuts",  0),
        batting.get("stolenBases", 0),
        batting.get("leftOnBase",  0),

        _str_innings_to_outs(pitching.get("inningsPitched", "0.0")),
        _get_era(pitching),                      # ERA as float or None
        pitches,
        pitching.get("strikes", 0)
    )


def process_team_box(box: Dict[str, Any], gamePk: int, cur) -> None:
    """
    Insert one row per club into `team_box`.
    """
    rows: List[TeamRow] = []

    for side in ("away", "home"):
        rows.append(
            map_team_box(gamePk, side, box[side])
        )

    # sanity peek
    #print("TBOX", rows)
    
    execute_values(cur, """
        INSERT INTO team_box (
            gamePk, team_id, is_home,
            runs, hits, doubles, triples, hr, rbi,
            bb, so, sb, lob,
            innings_pitched, era, pitches, strikes
        ) VALUES %s
        ON CONFLICT (gamePk, team_id) DO NOTHING;
    """, rows)

# ───────────────────────────────────────────
#  T E A M   F I E L D I N G   (raw JSON)
# ───────────────────────────────────────────
from typing import List, Tuple, Dict, Any

TeamFldRow = Tuple[int, int, bool, int, int, int, int]
#             gamePk, team_id, is_home, PO,  A,  E,  DP


def map_team_fielding_raw(gamePk: int,
                          side: str,
                          team_block: Dict[str, Any]) -> TeamFldRow:
    """
    `team_block` is data["teams"][side]
    """
    fld = team_block["teamStats"]["fielding"]

    return (
        gamePk,
        team_block["team"]["id"],
        side == "home",
        fld.get("putOuts",     0),
        fld.get("assists",     0),
        fld.get("errors",      0),
        fld.get("doublePlays", 0)
    )


def process_team_fielding(data: Dict[str, Any], gamePk: int, cur) -> None:
    """
    Insert one row per club into `team_fielding`.
    """
    rows: List[TeamFldRow] = [
        map_team_fielding_raw(gamePk, "away", data["teams"]["away"]),
        map_team_fielding_raw(gamePk, "home", data["teams"]["home"])
    ]

    # sanity peek
    #print("TFLD", rows)

    execute_values(cur, """
        INSERT INTO team_fielding (
            gamePk, team_id, is_home,
            putouts, assists, errors, double_plays
        ) VALUES %s
        ON CONFLICT (gamePk, team_id) DO NOTHING;
    """, rows)

# ────────────────────────────────────────────────────────────────
#  P L A Y E R   &   P L A Y E R _ T E A M   L O A D E R
# ────────────────────────────────────────────────────────────────
from typing import Dict, Any, List, Tuple, Iterable, Set
import statsapi, datetime
from psycopg2.extras import execute_values


# ----------  SAFER / BATCHED PEOPLE LOOKUP  ----------
def _safe_people_lookup(pid_batch: List[int],
                        max_retries: int = 4,
                        delay: int = 2) -> Dict[int, Any]:
    """
    One StatsAPI call can take a *comma-separated* list of ids, so we amortise
    network time and respect rate limits.
    """
    joined = ",".join(map(str, pid_batch))
    for attempt in range(max_retries):
        try:
            raw = statsapi.get("people", {"personIds": joined})
            return {p["id"]: p for p in raw["people"]}
        except Exception as e:
            print(f"[people] retry {attempt+1}/{max_retries} – {e}")
            time.sleep(delay * (attempt+1))
    return {}          # give up – caller will skip those players


# ----------  PLAYER TABLE ----------
PlayerRow = Tuple[int, str, str, str, str, datetime.date | None]

def _player_rows_from_people(people: Dict[int, Any]) -> List[PlayerRow]:
    rows: List[PlayerRow] = []
    for p in people.values():
        rows.append((
            p["id"],
            p["fullName"],
            p.get("primaryPosition", {}).get("abbreviation", None),
            p.get("batSide", {}).get("code", None),
            p.get("pitchHand", {}).get("code", None),
            datetime.date.fromisoformat(p["birthDate"]) if p.get("birthDate") else None
        ))
    return rows


# ----------  PLAYER_TEAM TABLE ----------
TeamRow = Tuple[int, int, int, int, int]
#           pid, season, team_id, first_gamePk, last_gamePk


def process_players(box: Dict[str, Any], gamePk: int, cur,
                    cache: Set[int]) -> None:
    """
    Ensures every player appearing in `box` is present in the `player` table.

    Parameters
    ----------
    box   : wrapper JSON from statsapi.boxscore_data()
    cache : a Python set carried by the caller so we never re-look-up a
            player id that has already been written this run.
    """
    # 1. gather *new* ids (not yet in cache)
    new_ids: List[int] = []
    for side in ("away", "home"):
        new_ids.extend([pid for pid in box[side]["players"]            # dict keys ‘ID<id>’
                               if (pid_int := int(pid[2:])) not in cache])
    if not new_ids:
        return

    # 2. hit Stats API in batches of 100 (safe upper bound)
    rows_to_insert: List[PlayerRow] = []
    BATCH = 100
    for i in range(0, len(new_ids), BATCH):
        batch_ids = [int(pid[2:]) for pid in new_ids[i:i+BATCH]]
        people_map = _safe_people_lookup(batch_ids)
        rows_to_insert.extend(_player_rows_from_people(people_map))
        cache.update(people_map.keys())        # mark as known

    # 3. upsert
    if rows_to_insert:
        execute_values(cur, """
            INSERT INTO player (
                player_id, full_name, primary_pos, bats, throws, birth_date
            ) VALUES %s
            ON CONFLICT (player_id) DO NOTHING;
        """, rows_to_insert)


def process_player_teams(box: Dict[str, Any],
                         gamePk: int,
                         season_year: int,
                         cur) -> None:
    """
    Maintains first/last-appearance windows for each (player, season, team).

    Uses a read-then-upsert pattern to respect the composite PK defined
    in your schema.
    """
    for side in ("away", "home"):
        team_id = box[side]["team"]["id"]
        for pid_key in box[side]["players"].keys():
            pid = int(pid_key[2:])

            # Does a row already exist?
            cur.execute("""
                SELECT first_gamePk, last_gamePk
                FROM player_team
                WHERE player_id = %s
                  AND season_year = %s
                  AND team_id = %s
                ORDER BY first_gamePk
                LIMIT 1;
            """, (pid, season_year, team_id))
            found = cur.fetchone()

            if found:
                # update only if this appearance is later
                if gamePk > found[1]:
                    cur.execute("""
                        UPDATE player_team
                           SET last_gamePk = %s
                         WHERE player_id = %s
                           AND season_year = %s
                           AND team_id = %s
                           AND first_gamePk = %s;
                    """, (gamePk, pid, season_year, team_id, found[0]))
            else:
                # new stint
                cur.execute("""
                    INSERT INTO player_team (
                        player_id, season_year, team_id, first_gamePk, last_gamePk
                    )
                    VALUES (%s, %s, %s, %s, %s);
                """, (pid, season_year, team_id, gamePk, gamePk))
