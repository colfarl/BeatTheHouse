'''
File to look at what is returned by function in utils
'''

# get_schedule()
game_in_schedule = {
    "game_id": 779129,  # maps to game.gamePk
    "game_datetime": "2025-03-01T18:05:00Z",  # maps to game.first_pitch_ts
    "game_date": "2025-03-01",  # maps to game.game_date
    "status": "Final",  # useful for filtering completed games

    "home_name": "Baltimore Orioles",
    "away_name": "Pittsburgh Pirates",
    "home_id": 110,  # maps to game.home_team_id
    "away_id": 134,  # maps to game.away_team_id

    "home_score": 2,  # maps to game.home_score
    "away_score": 5,  # maps to game.away_score

    "venue_id": 2508,
    "venue_name": "Ed Smith Stadium",  # maps to game.venue

    "doubleheader": "N",  # optional but may help in edge cases
    "game_num": 1,  # needed if dealing with DH1/DH2 logic

    "home_probable_pitcher": "Cade Povich",
    "away_probable_pitcher": "Paul Skenes",
    "home_pitcher_note": "",
    "away_pitcher_note": "",

    "winning_team": "Pittsburgh Pirates",  # derived from scores, redundant if stored
    "losing_team": "Baltimore Orioles",
    "winning_pitcher": "Tanner Rainey",  # these map to player stats
    "losing_pitcher": "Brandon Young",
    "save_pitcher": "Eddy Yean",

    "national_broadcasts": ["MLBN (out-of-market only)"],  # not stored in schema but useful for later media features
    "game_type": "S",  # "R" for regular season, "S" for spring, etc.
    "series_status": "PIT wins Spring",
    "summary": "2025-03-01 - Pittsburgh Pirates (5) @ Baltimore Orioles (2) (Final)"
}

#get_team() need to index [0] for exact match 
general_team = {
    'id': 147, 
    'name': 'New York Yankees',
    'teamCode': 'nya',
    'fileCode': 'nyy',
    'teamName': 'Yankees',
    'locationName': 'Bronx',
    'shortName': 'NY Yankees'
}
