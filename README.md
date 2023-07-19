# Manhattan Data Analytics Platform - README

## Introduction

The Manhattan Data Analytics Platform is a powerful solution developed by Patrick, the Data Engineer at Manhattan Inc. The platform was designed to streamline data analytics for Manhattan's marketing and sales teams, providing them with ad-hoc querying capabilities and easy access to historical data.

## Problem Statement

Manhattan's marketing and sales teams faced challenges in making ad-hoc queries from their CRM tool, which lacked this functionality. The process of manually downloading CSV files from the CRM tool, loading them into Excel, and performing queries was time-consuming and error-prone. Patrick, the data engineer, recognized the need to simplify this process and provide a more efficient way for the teams to access and analyze their data.

## Solution: ETL Task with Airflow

Patrick implemented an ETL (Extract, Transform, Load) task using Apache Airflow to automate the data loading process from CSV files to a MySQL database, acting as the data warehouse. The ETL workflow includes custom operators, namely the `CSVToMySQLOperator`, and a `FileSensor`. The `FileSensor` ensures that the required CSV file is available before initiating the data loading process.

### ETL Workflow

![Successful DAG RUN](./Successful_Dag_Run.png)

1. **FileSensor**: The `FileSensor` component checks if the CSV file is available for processing. It waits until the file becomes available before proceeding to the next step.

2. **CSVToMySQLOperator**: The `CSVToMySQLOperator` reads the data from the CSV file and efficiently loads it into the MySQL database. This operator automates the data transformation and loading process, saving time and effort.

## Solution: Flask App

[API DOCUMENTATION](https://documenter.getpostman.com/view/10629518/2s946ibrCP)

To provide the marketing and sales teams with a user-friendly interface for querying the MySQL database, Patrick developed a Flask app.

### Flask App Features

1. **Query Endpoint**: The Flask app features an endpoint that accepts SQL queries as input. Users can enter their ad-hoc queries through this interface and receive the query results as JSON.

2. **Data Saving Option**: The app also offers an option to save the query results in an S3 bucket. By specifying a save name, users can easily retrieve and reference the results in the future.

3. **Data Retrieval Endpoint**: Another endpoint in the Flask app enables users to retrieve the saved data from the S3 bucket. This feature ensures quick access to historical query results.

## Conclusion

Thanks to Patrick's ingenious solution, Manhattan's marketing and sales teams can now effortlessly perform ad-hoc queries using the Manhattan Data Analytics Platform. The Airflow-powered ETL task automates the loading of CSV data into the MySQL database, while the Flask app offers a user-friendly interface for querying the data. Additionally, the app's S3 integration allows for the easy retrieval of past query results, streamlining the analytics process and empowering the teams to make data-driven decisions with ease.

## Getting Started

To get started with the Manhattan Data Analytics Platform, follow these steps:
1. git clone the project
2. setup a .env file with the following configs
```angular2html
#DB Config
DB_HOST=localhost
DB_PORT=3306
DB_NAME=manhattan_db
STAGING_DB_SCHEMA=staging
ANALYTICS_DB_SCHEMA=analytics
DB_USER=<your_user>
DB_PASSWORD=<your_password>

#S3 Config
AWS_ACCESS_KEY=<your_access_key>
AWS_SECRET_KEY=<your_secret_key>
S3_BUCKET_NAME=manhattan-app-saved-queries
S3_REGION=<your_region>

# Manhttan Database Config
MYSQL_DATABASE=manhattan_db
MYSQL_ROOT_PASSWORD=<your_root_password>

#Airflow
# Airflow Core
AIRFLOW__CORE__FERNET_KEY=UKMzEm3yIuFYEq1y3-2FxPNWSVwRASpahmQ9kQfEr8E=
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW_UID=0

# Backend DB
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow_db/airflow
AIRFLOW__DATABASE__LOAD_DEFAULT_CONNECTIONS=False

# Airflow Init
_AIRFLOW_DB_UPGRADE=True
_AIRFLOW_WWW_USER_CREATE=True
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```
3. Create a virtual environmen and run `pip3 install -r requirements.tct`
4. Start the backend services as containsers by running `docker-compose up`
5. Run the Flask app by running `python3 app.py`.
6. Access the Flask app through its endpoints ([API DOCUMENTATION](https://documenter.getpostman.com/view/10629518/2s946ibrCP)) to query the MySQL database and save/retrieve data from the S3 bucket.

Enjoy the efficiency and convenience of the Manhattan Data Analytics Platform, designed to transform data analysis for your business!
