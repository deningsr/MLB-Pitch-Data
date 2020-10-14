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
