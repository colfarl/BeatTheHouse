-- ─────────────────────────────
--  CORE REFERENCE TABLES
-- ─────────────────────────────
CREATE TABLE IF NOT EXISTS team (
    team_id       SMALLINT,
    season_year   SMALLINT,
    name          TEXT,
    abbr          CHAR(3),
    league        CHAR(2),
    division      CHAR(2),
    PRIMARY KEY (team_id, season_year)
);

CREATE TABLE IF NOT EXISTS player (
    player_id     INTEGER PRIMARY KEY,
    full_name     TEXT,
    primary_pos   CHAR(2),
    bats          CHAR(1),
    throws        CHAR(1),
    birth_date    DATE
);

CREATE TABLE IF NOT EXISTS player_team (
    player_id     INTEGER,
    season_year   SMALLINT,
    team_id       SMALLINT,
    first_gamePk  BIGINT,
    last_gamePk   BIGINT,
    PRIMARY KEY (player_id, season_year, team_id, first_gamePk)
);

-- ─────────────────────────────
--  GAME META
-- ─────────────────────────────
CREATE TABLE IF NOT EXISTS game (
    gamePk         BIGINT PRIMARY KEY,
    season_year    SMALLINT,
    game_date      DATE,
    venue          TEXT,
    home_team_id   SMALLINT,
    away_team_id   SMALLINT,
    first_pitch_ts TIMESTAMPTZ,
    attendance     INTEGER,
    weather_temp_f SMALLINT,
    wind_mph       SMALLINT,
    home_score     SMALLINT,
    away_score     SMALLINT
);

-- ─────────────────────────────
--  TEAM-LEVEL BOX / FIELDING
-- ─────────────────────────────
CREATE TABLE IF NOT EXISTS team_box (
    gamePk   BIGINT,
    team_id  SMALLINT,
    is_home  BOOLEAN,
    runs     SMALLINT,
    hits     SMALLINT,
    doubles  SMALLINT,
    triples  SMALLINT,
    hr       SMALLINT,
    rbi      SMALLINT,
    bb       SMALLINT,
    so       SMALLINT,
    sb       SMALLINT,
    lob      SMALLINT,
    innings_pitched SMALLINT,
    era      NUMERIC(4,2),
    pitches  SMALLINT,
    strikes  SMALLINT,
    PRIMARY KEY (gamePk, team_id)
);

CREATE TABLE IF NOT EXISTS team_fielding (
    gamePk   BIGINT,
    team_id  SMALLINT,
    is_home  BOOLEAN,
    putouts      SMALLINT,
    assists      SMALLINT,
    errors       SMALLINT,
    double_plays SMALLINT,
    PRIMARY KEY (gamePk, team_id)
);

-- ─────────────────────────────
--  PLAYER LINES
-- ─────────────────────────────
CREATE TABLE IF NOT EXISTS player_batting (
    gamePk       BIGINT,
    player_id    INTEGER,
    team_id      SMALLINT,
    batting_order SMALLINT,
    position     CHAR(3),
    ab  SMALLINT, r  SMALLINT, h  SMALLINT,
    "2b" SMALLINT, "3b" SMALLINT, hr SMALLINT,
    rbi SMALLINT, bb SMALLINT, so SMALLINT,
    sb  SMALLINT, hbp SMALLINT, lob SMALLINT,
    PRIMARY KEY (gamePk, player_id)
);

CREATE TABLE IF NOT EXISTS player_pitching (
    gamePk       BIGINT,
    player_id    INTEGER,
    team_id      SMALLINT,
    is_starting  BOOLEAN,
    outs_recorded SMALLINT,
    bf  SMALLINT, h  SMALLINT, r  SMALLINT,
    er SMALLINT, bb SMALLINT, so SMALLINT,
    hr SMALLINT, pitches SMALLINT, strikes SMALLINT,
    win_flag  BOOLEAN, save_flag BOOLEAN, hold_flag BOOLEAN,
    PRIMARY KEY (gamePk, player_id)
);

CREATE TABLE IF NOT EXISTS player_fielding (
    gamePk       BIGINT,
    player_id    INTEGER,
    team_id      SMALLINT,
    position     CHAR(3),
    putouts      SMALLINT,
    assists      SMALLINT,
    errors       SMALLINT,
    double_plays SMALLINT,
    PRIMARY KEY (gamePk, player_id)
);


--More to come, need to add odds data and perhaps get more detailed statcast tables
