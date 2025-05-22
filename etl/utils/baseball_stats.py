import statsapi

def get_schedule(year: int):
    return statsapi.schedule(start_date=f"{year}-03-01", end_date=f"{year}-11-30")

def get_team(id):
    res = statsapi.lookup_team(id)
    if len(res) == 0:
        print(id)
    return res

def get_box(gamePk):
    return statsapi.boxscore_data(gamePk, timecode=None)