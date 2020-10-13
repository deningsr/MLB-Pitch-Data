create_pitches_table = f"""
    DROP TABLE IF EXISTS pitches;
    CREATE TABLE pitches (
        pitcher_id varchar,
        batter_id varchar,
        ab_id varchar,
        ab_result text,
        g_id varchar,
        p_throw varchar,
        pitch_result varchar,
        pitch_type varchar,
        Batter's Name varchar.
        Pitcher's Name varchar
    );
    """

create_games_table = f"""
    DROP TABLE IF EXISTS games;
    CREATE TABLE games (
        attendance varchar,
        away_final text,
        home_final text,
        date text,
        elapsed_time text,
        g_id varchar,
        home_final,
        home_team varchar,
        start_time varchar,
        weather varchar
    );
    """