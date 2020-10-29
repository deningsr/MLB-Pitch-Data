from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.plugins.operators.Download_data_operator import DownloadDataOperator
import logging
from airflow.plugins.operators.Has_rows_operator import CheckHasRowsOperator
from airflow.plugins.operators.Has_future_years_operator import CheckFutureYearsOperator
from airflow.plugins.helpers import sql_queries
from airflow.hooks.postgres_hook import PostgresHook

import os
from datetime import datetime
import re

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


default_args = {
    "start_date": datetime(2019, 1, 1),
    "max_active_runs": 1,
    "retries": 3
}

dag = DAG(
    dag_id="mlb_data_pipeline",
    schedule_interval="@weekly",
    catchup=False,
    default_args=default_args
)

def Transform_pitch_data(pitches_file, atbats_file, names_file, spark_output_dir):

    """
    This function transforms the pitches dataset and returns a new csv file.
    There are three files are required to create the complete pitches dataset and they are taken
    as arguments to the function.
    """

    # Creates our SparkSession
    spark = (
        SparkSession
        .builder
        .master("local")
        .appName("MLB-pitch-data")
        .getOrCreate()
    )
    if os.path.exists(spark_output_dir):
        rmtree(spark_output_dir)

    # Loads datasets into Spark dataframes and transforms them
    logging.info("loading pitches dataset to SparkSession")
    pitches_df = spark.read.csv(pitches_file, header=True)
    atbats_df = spark.read.csv(atbats_file, header=True)
    names_df = spark.read.csv(names_file, header=True)
    pitch_types = {
        "CH": "Changeup", "CU": "Curveball", "EP": "Eephus", "FC": "Cutter",
        "FF": "Four-seam Fastball", "FO": "Pitchout (also PO)", "FS": "Splitter",
        "FT": "Two-seam Fastball", "IN": "intentional ball", "KC": "Knuckle curve",
        "KN": "Knuckleball", "PO": "Pitchout (also FO)", "SC": "Screwball",
        "SI": "Sinker", "SL": "Slider", "UN": "Unknown"
    }
    
    pitch_results = {
        "B": "Ball", "*B": "Ball in dirt", "S": "Swinging Strike",
        "C": "Called Strike", "F": "Foul", "T": "Foul Tip",
        "L": "Foul Bunt", "I": "Intentional Ball", "W": "Swinging Strike (Blocked)",
        "M": "Missed Bunt", "P": "Pitchout", "Q": "Swinging Pitchout",
        "R": "Foul Pitchout", "X": "In play, out(s)", "D": "In play, no out",
        "E": "In play, runs", "H": "Hit by pitch"
    }
 
    
    names_df = names_df.selectExpr("id as batter_id", "first_name as fname", "last_name as lname")
    
    df1 = atbats_df.join(pitches_df, on='ab_id', how='left')
    
    df2 = df1.join(names_df, on='batter_id', how='left')
    
    df2 = df2.withColumn("batter_name", F.concat(F.col('fname'),F.lit(' '),F.col('lname'))).drop('fname', 'lname')
    df2 = df2.drop('spin_rate', 'spin_dir', 'end_speed', 'start_speed', 'inning', 'top', 'type', 'p_score', 'o', 'stand', 'b_score',\
                    'break_angle', 'break_length', 'break_y', 'ax', 'ay', 'pz', 'outs', 'zone', 'nasty', 'sz_top', 'event_num', 'b_count', 's_count',\
                    'pitch_num', 'on_3b', 'on_2b', 'on_1b', 'az', 'sz_bot', 'sx_top', 'type_confidence', 'vx0', 'px', 'vy0', 'vz0', 'x', 'x0', 'y', 'y0', 'z0', 'pfx_x', 'pfx_z')
    df2 = df2.withColumnRenamed("event", "ab_result")
    df2 = df2.withColumnRenamed("code", "pitch_result")

    names_df = names_df.selectExpr("batter_id as pitcher_id", 'fname as fname', 'lname as lname')
    final_df = df2.join(names_df, on='pitcher_id', how='left')
    final_df = final_df.withColumn("pitcher_ame", F.concat(F.col('fname'),F.lit(' '),F.col('lname'))).drop('fname', 'lname')
   
    pitches = final_df.replace(to_replace=pitch_types, subset=['pitch_type'])
    pitches = final_df.replace(to_replace=pitch_results, subset=['code'])
    pitches.printSchema()
    
    # exports new pitches csv
    (
        pitches
        .repartition(1)
        .write
        .option("delimiter", ",")
        .csv(spark_output_dir, mode="overwrite")
    )


def transform_games_data(input_file, spark_output_dir):

    """
    This function transforms the games dataset and returns a new csv file.
    """

    spark = ( 
        SparkSession
        .builder
        .master("local")
        .appName("MLB-pitch-data")
        .getOrCreate()
    )

    if os.path.exists(spark_output_dir):
        rmtree(spark_output_dir)
    # Loads dataset into Spark dataframe and returns a new csv
    logging.info('loading games dataset into SparkSession')
    games = spark.read.csv(input_file, header=True)

    games.withColumn("away_team", F.upper(F.col('home_team')))
    games.withColumn("home_team", F.upper(F.col('away_team')))
    games = games.drop('umpire_3B', 'umpire_2B', 'umpire_1B', 'umpire_HP', 'venue_name', 'wind', 'delay')
    games.printSchema()

    (
        games
        .repartition(1)
        .write
        .csv(output_file, mode="overwrite")
    )

transform_games_data('games.csv', 'output.csv')

def load_spark_csv_to_postgres(spark_csv, conn_id, table):

    """
    Loads Spark csv files into Postgres tables.
    """

    # Get Spark CSV file path
    spark_csv_file_name = list(filter(
        re.compile("part-.*csv$").match, os.listdir(spark_csv)
    ))[0]
    spark_csv_file_path = f"{spark_csv}/{spark_csv_file_name}"

    # Get Postgres hook
    postgres_hook = PostgresHook(conn_id)
    
    # Clear Postgres table
    print(f"Clearing Postgres table {table}")
    postgres_hook.run(f"truncate {table}")
    
    
    postgres_hook.bulk_load(table, spark_csv)


download_pitches_task = Download_data_operator(
    task_id="download_pitches",
    url="https://drive.google.com/uc?export=download&id=12I9UQBkepS9MDKQhC1Lambc82kq9Yf4R",
    dir='data',
    dag=dag
)

transform_pitches_task = PythonOperator(
    task_id="transform_pitches",
    python_callable=transform_pitch_data,
    ptiches_file="/data/pitches.csv",
    atbats_file="/data/atbats.csv",
    names_file="/data/player_names.csv",
    output_file="/data/output.csv",
    dag=dag
)

transform_games_task = PythonOperator(
    task_id="transform_pitches",
    python_callable=transform_pitch_data,
    input_file="/data/games.csv",
    output_filel="/data/output.csv",
    dag=dag
)

create_pitches_in_postgres = PostgresOperator(
    task_id="create_pitches_in_postgres",
    sql=sql_queries.create_pitches,
    conn_id="postgres",
    dag=dag
)

create_games_in_postgres = PostgresOperator(
    task_id="create_games_in_postgres",
    sql=sql_queries.create_games,
    conn_id="postgres",
    dag=dag
)

load_pitches_to_postgres = PythonOperator(
    task_id="load_pitches_to_postgres",
    python_callable=load_spark_csv_to_postgres,
    spark_csv="/pitches",
    conn_id="postgres",
    table="pitches",
    dag=dag
)

load_games_to_postgres = PythonOperator(
    task_id="load_games_to_postgres",
    python_callable=load_spark_csv_to_postgres,
    spark_csv="/games",
    conn_id="postgres",
    table="games",
    dag=dag
)

check_pitche_has_rows = CheckHasRowsOperator(
    task_id="check_pitches_has_rows",
    conn_id="postgres",
    table="pitches",
    dag=dag
)

check_games_has_rows = CheckHasRowsOperator(
    task_id="check_games_has_rows",
    conn_id="postgres",
    table="games",
    dag=dag
)


check_population_future_years = CheckFutureYearsOperator(
    task_id="check_games_future_years",
    postgres_conn_id="postgres",
    postgres_table_name="games",
    dag=dag
)

download_pitches_task >> transform_pitches_task
transform_pitches_task >> load_pitches_to_postgres
create_pitches_in_postgres >> load_pitches_to_postgres
load_pitches_to_postgres >> check_pitches_has_rows

transform_games_task >> load_games_to_postgres
create_games_in_postgres >> load_games_to_postgres
load_games_to_postgres >> check_games_has_rows
load_games_to_postgres >> check_games_future_years
