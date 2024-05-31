# PSSP-Cloud-SQL + Dumm Data

## HHA 504 // Week 7 // Assignment 6

### Description

In this repository, we continue our work with building a patient portal within a Flask app. We create tables in a GCP Database for MySQL Server remotely through a `create_engine` connection with dotenv credentials. Then, we populate these empty tables with data found on the Internet pertaining to unique identifier codes. The sole exception is the `patients` table, which we populate using Faker, an imported package designed to generate random person data.

Through our local terminals, we can check our work by accessing our GCP Database for MySQL Server and manually querying our databases. Finally, after installing MySQL Workbench, we can reverse engineer entity relationship diagrams (ERD) to create a visual indicator of foreign key connections between tables.

### Python Components

There are two main Python components to this repository:

1. **`sql_table_creation.py`**
   - This Python script accesses the GCP Database for MySQL via dotenv credentials and creates the following tables within the `patient_portal` database:
     - `patients`
     - `medications`
     - `treatment_procedures`
     - `conditions`
     - `social_determinants`
     - `patient_medications`
     - `patient_treatment_conditions`
     - `patient_conditions`
     - `patient_social_determinants`

2. **`sql_dummy_data.py`**
   - This Python script uses imported packages to populate tables created by `sql_table_creation.py` with dummy data and randomized samples of publicly available CSVs.
