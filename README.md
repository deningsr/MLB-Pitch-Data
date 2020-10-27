# MLB Pitch Data

#### MLB Pitch Data is my capstone project for the Udacity Data Engineering Nanodegree Program. Its purpose is to display what I have learned during the course and showcase my skills. I have chosen to focus on using big data for this project by utilizing technologies such Spark, PostgresSQL, and Airflow to create a working ETL pipeline.

## Purpose

#### This project is intended to provide sports analysts, scouts, and baseball enthusiasts with a wealth of pitch-level data for MLB games between 2015-2018. The data can support these parties with trends on pitches, batters, and pitch types and can help answer a variety of questions.

## Data Sources

#### The source data is taken from the MLB Pitch Data 2015-2018 found on Kaggle.com. Several individual datasets are pulled from this source and are detailed below:

* MLB Pitch Data: Contains pitch-level data such as pitcher, batter, pitch type, speed of pitch, and result of the pitch (strike, ball, out, etc..)

* MLB at-bat Data: Contains at-bat-level data such as pitcher, batter, at-bat result, and current inning.

* MLB Games Data: Contains game-level data such as home team, away team, date, and final score.

* MLB Player Names Data: Contains first and last names of pitchers and batters. Used as a helper dataset to join player names.

## Technologies Used

* **Spark** is used in the ETL process to transform and output the data because it can handle the large volume used.

* **Python** will be used as the main language due to the availability of PySpark and other packages making it a flexible option. It is also the language I am most familiar with.

* **PostgresSQL** will be used as the relational database that will hold the final data. It it as good choice to store to tabular data in csv files

* **Airflow** will be used as it is the premier tool for maintaining ETL pipelines and will allow this project to be maintained and automated.

## Data Model

#### PostgresSQL will suffice to store the data as it sits in tabular format in csv files. PostgresSQL will also manage the size of the data as well. The database will contain two tables: Pitches and Games.

1. Pitches:
    * <code>pitcher_id:</code> Unique key of each pitcher in the dataset.
    * batter_id: Unique key of each batter in the dataset.
    * ab_id: Unique key of each at-bat in the dataset.
    * ab_result: Result of the atbat (out, walk, run, etc..).
    * g_id: Unique key of each game in the dataset.
    * p_throw: Arm the pitcher is using to throw.
    * pitch_result: Result of the pitch (ball, strike, out, etc..).
    * pitch_type: Type of the pitch (fastball, curveball, slider, etc..).
    * Batter's Name: First and last name of the batter
    * Pitcher's Name: First and last name of the pitcher

2. Games:
    * attendance: Crowd attendance at the game.
    * away_final_score: Final score of the visiting team.
    * away_team: Name of the visiting team.
    * date: Date of the game.
    * elapsed_time: Total duration of the game.
    * g_id: Unique key of each game.
    * home_final_score: Final score of the home team.
    * home_team: Name of the home team.
    * start_time: Start time of game.
    * weather: Weather at the first pitch of the game.

## Alternate Scenarios

* **Data increased by 100x:** This project could be vertically scaled to accomodate such an increase of data. To implement this scaling, I would utilize a cloud storage option such as Amazon S3 and a distributed server like Redshift. Spark would still be acceptable to use in this case.

* **Pipleline being run on a daily basis:** This is easily integrated in the process due to the use of Airflow. The DAG parameters would simply need to be adjusted to accomodate any changes in the running frequency.

* **Mulitple parties required to access the data:** The solution to the first scenario would also satisfy this requirement as horizontally integrating this project on the cloud would allow any number of remote parties to access the data as ofthen as they so choose.