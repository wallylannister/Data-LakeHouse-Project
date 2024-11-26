# Project Title
Cloud Data Lakehouse ETL Pipeline

## Project Description
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:
- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.
Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.
The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.
Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

As a data engineer on the STEDI Step Trainer team, you'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

## Steps
1. Create S3 directories for customer_landing, step_trainer_landing, and accelerometer_landing.
    - Objective: This simulates the data sources engineers will be working with, allowing them to organize and manage the incoming data effectively.
2. Write SQL scripts to create two Glue tables for the customer_landing and accelerometer_landing zones and share these scripts in Git.
    - Objective: Glue tables help in structuring the semi-structured data, making it easier to query and analyze using AWS Athena.
3. Use Athena to query the tables created and take screenshots of the results.
    - Objective: This step verifies that the data is correctly ingested and structured in the Glue tables, ensuring data integrity before further processing.
4. Create two Glue jobs to sanitize the customer and accelerometer data, filtering out records that do not meet the criteria (e.g., customers who agreed to share their data).
    - Objective: This ensures that only trusted data is retained for analysis, maintaining compliance with data privacy regulations.
5. Verify Glue Job Success by querying the customer_trusted table with Athena and take a screenshot of the results.
    - Objective: This step confirms that the Glue jobs executed successfully and that the data meets the required conditions.
6. Handle Data Quality Issues by writing a Glue job to create a curated table that includes only customers with accelerometer data and who agreed to share their data.
    - Objective: This addresses the data quality issue related to duplicate serial numbers, ensuring that the curated data is accurate and useful for analysis.
7. Create Glue Studio jobs to read the Step Trainer IoT data and populate a trusted zone Glue table, and to create an aggregated table for machine learning.
    - Objective: These jobs facilitate the integration of various data sources, allowing for comprehensive analysis and insights.
8. Use Athena to query the curated Glue tables and take screenshots of the results.
    - Objective: This final verification step ensures that the data is correctly aggregated and ready for any downstream analysis or machine learning tasks.

## Conclusion
Each step in this project is designed to ensure that data is properly structured, sanitized, and verified throughout the process. This systematic approach not only helps in maintaining data quality but also prepares the data for effective analysis and decision-making in the context of human balance analytics.
