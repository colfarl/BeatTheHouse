import statsapi
import re
import requests

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
def get_box(gamePk):
    return statsapi.boxscore_data(gamePk, timecode=None)


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