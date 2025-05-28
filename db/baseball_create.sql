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

CREATE TABLE IF NOT EXISTS savant_batter_stats (
    "player_id" INTEGER,
    "year" INTEGER,
    "last_name, first_name" TEXT,
    "player_age" INTEGER,
    "ab" INTEGER,
    "pa" INTEGER,
    "hit" INTEGER,
    "single" INTEGER,
    "double" INTEGER,
    "triple" INTEGER,
    "home_run" INTEGER,
    "strikeout" INTEGER,
    "walk" INTEGER,
    "k_percent" NUMERIC(8, 3),
    "bb_percent" NUMERIC(8, 3),
    "batting_avg" NUMERIC(8, 3),
    "slg_percent" NUMERIC(8, 3),
    "on_base_percent" NUMERIC(8, 3),
    "on_base_plus_slg" NUMERIC(8, 3),
    "isolated_power" NUMERIC(8, 3),
    "babip" NUMERIC(8, 3),
    "b_rbi" INTEGER,
    "xba" NUMERIC(8, 3),
    "xslg" NUMERIC(8, 3),
    "woba" NUMERIC(8, 3),
    "xwoba" NUMERIC(8, 3),
    "xobp" NUMERIC(8, 3),
    "xiso" NUMERIC(8, 3),
    "exit_velocity_avg" NUMERIC(8, 3),
    "sweet_spot_percent" NUMERIC(8, 3),
    "barrel_batted_rate" NUMERIC(8, 3),
    "hard_hit_percent" NUMERIC(8, 3),
    "avg_best_speed" NUMERIC(8, 3),
    "meatball_swing_percent" NUMERIC(8, 3),
    "iz_contact_percent" NUMERIC(8, 3),
    "whiff_percent" NUMERIC(8, 3),
    "swing_percent" NUMERIC(8, 3),
    PRIMARY KEY (player_id, year)
);

CREATE TABLE IF NOT EXISTS savant_pitcher_stats (
    player_id INTEGER,
    year SMALLINT,
    player_age SMALLINT,
    p_game INTEGER,
    p_formatted_ip NUMERIC(6, 2),
    pa INTEGER,
    ab INTEGER,
    hit INTEGER,
    single INTEGER,
    double INTEGER,
    triple INTEGER,
    home_run INTEGER,
    strikeout INTEGER,
    walk INTEGER,
    k_percent NUMERIC(5, 2),
    bb_percent NUMERIC(5, 2),
    batting_avg NUMERIC(5, 3),
    slg_percent NUMERIC(5, 3),
    p_earned_run INTEGER,
    p_blown_save INTEGER,
    p_out INTEGER,
    p_win INTEGER,
    p_loss INTEGER,
    p_balk INTEGER,
    p_era NUMERIC(5, 2),
    p_opp_on_base_avg NUMERIC(5, 3),
    p_total_stolen_base INTEGER,
    p_pickoff_attempt_1b INTEGER,
    p_pickoff_attempt_2b INTEGER,
    p_pickoff_attempt_3b INTEGER,
    p_pickoff_1b INTEGER,
    p_pickoff_2b INTEGER,
    p_pickoff_3b INTEGER,
    p_pickoff_error_1b INTEGER,
    p_pickoff_error_2b INTEGER,
    p_pickoff_error_3b INTEGER,
    xba NUMERIC(5, 3),
    xslg NUMERIC(5, 3),
    woba NUMERIC(5, 3),
    xwoba NUMERIC(5, 3),
    xobp NUMERIC(5, 3),
    xiso NUMERIC(5, 3),
    meatball_percent NUMERIC(5, 2),
    pitch_count INTEGER,
    in_zone_percent NUMERIC(5, 2),
    whiff_percent NUMERIC(5, 2),
    f_strike_percent NUMERIC(5, 2),
    groundballs_percent NUMERIC(5, 2),
    flyballs_percent NUMERIC(5, 2),
    popups_percent NUMERIC(5, 2),
    n INTEGER,
    arm_angle NUMERIC(5, 2),
    n_fastball_formatted INTEGER, -- Altered to Numeric
    fastball_avg_speed NUMERIC(5, 2),
    fastball_avg_spin NUMERIC(7, 2),
    n_breaking_formatted INTEGER, -- Altered to Numeric
    breaking_avg_speed NUMERIC(5, 2),
    breaking_avg_spin NUMERIC(7, 2),
    n_offspeed_formatted INTEGER, -- Altered to Numeric
    offspeed_avg_speed NUMERIC(5, 2),
    offspeed_avg_spin NUMERIC(7, 2),
    PRIMARY KEY (player_id, year)
);

