# MLB Pitch Data

#### MLB Pitch Data is my capstone project for the Udacity Data Engineering Nanodegree Program. Its purpose is to display what I have learned during the course and showcase my skills. I have chosen to focus on using big data for this project by utilizing technologies such Spark, PostgresSQL, and Airflow to create a working ETL pipeline.

## Goal

#### This project is intended to provide sports analysts, scouts, and baseball enthusiasts with a wealth of pitch-level data for MLB games between 2015-2018. The data can support these parties with trends on pitches, batters, and pitch types and can help answer a variety of questions.

## Project Scope and Data Gathering

* The project will utilize MLB pitch and game data between 2015-2018 in the form of four csv files from Kaggle.com

* The most usable case for this data would be analytic tables to provide scouts and baseball enthusiasts with pitch-level data. They can run queries to determine which pitchers throw which pitches and and which batters are most successful.

## Exploring and Accessing the Data

* All datasets required some EDA prior to being used in the project. This included applying functions to several rows to clarify values.


## Data Sources

#### The source data is taken from the MLB Pitch Data 2015-2018 found on Kaggle.com. Several individual datasets are pulled from this source and are detailed below:

* MLB Pitch Data: Contains pitch-level data such as pitcher, batter, pitch type, speed of pitch, and result of the pitch (strike, ball, out, etc..). To access this csv, please refer to the direct link in the <code>download_pitches_task</code> in the DAG.

* MLB At-Bat Data: Contains at-bat-level data such as pitcher, batter, at-bat result, and current inning.

* MLB Games Data: Contains game-level data such as home team, away team, date, and final score.

* MLB Player Names Data: Contains first and last names of pitchers and batters. Used as a helper dataset to join player names.

## Technologies Used

* **Spark** is used in the ETL process to transform and output the data because it can handle the large volume used.

* **Python** will be used as the main language due to the availability of PySpark and other packages making it a flexible option. It is also the language I am most familiar with.

* **PostgresSQL** will be used as the relational database that will hold the final data. It it as good choice to store to tabular data in csv files

* **Airflow** will be used as it is the premier tool for maintaining ETL pipelines and will allow this project to be maintained and automated.

## Data Model and Data Dictionary

#### PostgresSQL was chosen as the best data model for several reasons. The data sits in tabular format in csv files and is fairly large. Postgres will also allow for ah hoc queries on the dataset. The database will contain two tables: Pitches and Games.

## Please refer to <code>sql_queries,py</code> for data constraints and types.
 
1. Pitches:
    * <code>pitcher_id</code>: Unique key of each pitcher in the dataset.
    * <code>batter_id</code>: Unique key of each batter in the dataset.
    * <code>ab_id</code>: Unique key of each at-bat in the dataset.
    * <code>ab_result</code>: Result of the atbat (out, walk, run, etc..).
    * <code>g_id</code>: Unique key of each game in the dataset.
    * <code>p_throw</code>: Arm the pitcher is using to throw.
    * <code>pitch_result</code>: Result of the pitch (ball, strike, out, etc..).
    * <code>pitch_type</code>: Type of the pitch (fastball, curveball, slider, etc..).
    * <code>batter_name</code>: First and last name of the batter
    * <code>pitcher_name:</code> First and last name of the pitcher

2. Games:
    * <code>attendance</code>: Crowd attendance at the game.
    * <code>away_final_score</code>: Final score of the visiting team.
    * <code>away_team</code>: Name of the visiting team.
    * <code>date</code>: Date of the game.
    * <code>elapsed_time:</code> Total duration of the game.
    * <code>g_id</code>: Unique key of each game.
    * <code>home_final_score</code>: Final score of the home team.
    * <code>home_team</code>: Name of the home team.
    * <code>start_time</code>: Start time of game.
    * <code>weather</code>: Weather at the first pitch of the game.

## Updading the Datasets

* This data could be updated annually after each MLB season is completed. Since the length of the season varies each year, it makes sense to perform annual updates.

## Sample Queries

#### What are the most common pitches thrown by each pitcher?

<code>
SELECT pitcher_name, pitch_type, COUNT(*)
FROM pitches
GROUP BY pitcher_name, pitch_type
ORDER BY COUNT(*) DESC;
</code>

#### Which batter's recorded the most hits during the period?

<code>
SELECT batter_name, COUNT(*)
FROM pitches
WHERE ab_result = 'hit'
GROUP BY batter_name
ORDER BY COUNT(*) DESC;
</code>

## Alternate Scenarios

* **Data increased by 100x:** This project could be vertically scaled to accomodate such an increase of data. To implement this scaling, I would utilize a cloud storage option such as Amazon S3 and a distributed server like Redshift. Spark would still be acceptable to use in this case.

* **Pipleline being run on a daily basis:** This is easily integrated in the process due to the use of Airflow. The DAG parameters would simply need to be adjusted to accomodate any changes in the running frequency.

* **Mulitple parties required to access the data:** The solution to the first scenario would also satisfy this requirement as horizontally integrating this project on the cloud would allow any number of remote parties to access the data as ofthen as they so choose.

## Running the Project

* Start up a local Postgres server

* Download the project directory exactly as it appears in the Github respository. This will ensure all modules are imported
correctly and allow the project to run correctly.

* Ensure <code>Airflow</code> is installed in the directory and run the DAG using <code>python mlb_data_dag.py</code> on the command line