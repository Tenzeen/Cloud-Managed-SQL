#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from faker import Faker # https://faker.readthedocs.io/en/master/
import uuid
import random

load_dotenv()
GCP_MYSQL_HOSTNAME = os.getenv("GCP_MYSQL_HOSTNAME")
GCP_MYSQL_USER = os.getenv("GCP_MYSQL_USERNAME")
GCP_MYSQL_PASSWORD = os.getenv("GCP_MYSQL_PASSWORD")
GCP_MYSQL_DATABASE = os.getenv("GCP_MYSQL_DATABASE")

connection_string_gcp = f'mysql+pymysql://{GCP_MYSQL_USER}:{GCP_MYSQL_PASSWORD}@{GCP_MYSQL_HOSTNAME}:3306/{GCP_MYSQL_DATABASE}'
db_gcp = create_engine(connection_string_gcp)

#show databases
print(db_gcp.table_names())

####### CREATING DUMMY DATA ########

#CREATE FAKE PATIENT INFORMATION
fake = Faker()
fake_patients = [
    {
        #keep just the first 8 characters of the uuid
        'mrn': str(uuid.uuid4())[:8], 
        'first_name':fake.first_name(), 
        'last_name':fake.last_name(),
        'zip_code':fake.zipcode(),
        'dob':(fake.date_between(start_date='-90y', end_date='-20y')).strftime("%Y-%m-%d"),
        'gender': fake.random_element(elements=('M', 'F')),
        'contact_mobile':fake.phone_number(),
        'contact_home':fake.phone_number()
    } for x in range(50)]

df_fake_patients = pd.DataFrame(fake_patients)
# drop duplicates
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn'])



#CREATE ICD10 CODES 
icd10codes = pd.read_csv('https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
list(icd10codes.columns)
icd10codesShort = icd10codes[['CodeWithSeparator', 'ShortDescription']]
icd10codesShort_1k = icd10codesShort.sample(n=1000)
# drop duplicates
icd10codesShort_1k = icd10codesShort_1k.drop_duplicates(subset=['CodeWithSeparator'], keep='first')



#CREATE NDC CODES
ndc_codes = pd.read_csv('https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
ndc_codes_1k = ndc_codes.sample(n=1000, random_state=1)
#drop duplicates
ndc_codes_1k = ndc_codes_1k.drop_duplicates(subset=['PRODUCTNDC'], keep='first')



#CREATE CPT CODES
cpt_codes = pd.read_csv('https://gist.githubusercontent.com/lieldulev/439793dc3c5a6613b661c33d71fdd185/raw/25c3abcc5c24e640a0a5da1ee04198a824bf58fa/cpt4.csv')
cpt_codes_1k = cpt_codes.sample(n=1000, random_state=1)
#drop duplicates
cpt_codes_1k = cpt_codes_1k.drop_duplicates(subset=['com.medigy.persist.reference.type.clincial.CPT.code'], keep='first')



#CREATE LOINC CODES
loinc_codes = pd.read_csv("data\Loinc.csv")
loinc_codes_short = loinc_codes[['LOINC_NUM', 'LONG_COMMON_NAME']]
loinc_codes_1k = loinc_codes_short.sample(n=1000, random_state=1) 
#drop duplicates
loinc_codes_1k = loinc_codes_1k.drop_duplicates(subset=['LOINC_NUM'], keep='first')



#CREATE FAKE PATIENT MEDICATIONS
df_medications = pd.read_sql_query("SELECT ndc_code FROM medications", db_gcp)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_gcp)

df_patient_medications = pd.DataFrame(columns=['mrn', 'ndc_code'])
for index, row in df_patients.iterrows():
    df_medications_sample = df_medications.sample(n=random.randint(1, 5))
    df_patient_medications = df_patient_medications.append(df_medications_sample)

df_patient_medications



#CREATE FAKE PATIENT CONDITIONS
df_conditions = pd.read_sql_query("SELECT icd10_code FROM conditions", db_gcp)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_gcp)

df_patient_conditions = pd.DataFrame(columns=['mrn', 'icd10_code'])
for index, row in df_patients.iterrows():
    df_conditions_sample = df_conditions.sample(n=random.randint(1, 5))
    df_conditions_sample['mrn'] = row['mrn']
    df_patient_conditions = df_patient_conditions.append(df_conditions_sample)

df_patient_conditions



#CREATE FAKE PATIENT TREATMENT/PROCEDURES
df_treatment_procedures = pd.read_sql_query("SELECT cpt_code FROM treatment_procedures", db_gcp)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_gcp)

df_patient_treatment_procedures = pd.DataFrame(columns=['mrn', 'cpt_code'])
for index, row in df_patients.iterrows():
    df_treatment_procedures_sample = df_treatment_procedures.sample(n=random.randint(1, 5))
    df_treatment_procedures_sample['mrn'] = row['mrn']
    df_patient_treatment_procedures = df_patient_treatment_procedures.append(df_treatment_procedures_sample)

df_patient_treatment_procedures

cpt_codes.shape

#CREATE FAKE PATIENT SOCIAL DETERMINANTS
df_social_determinants = pd.read_sql_query("SELECT loinc_code FROM social_determinants", db_gcp)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", db_gcp)

df_patient_social_determinants = pd.DataFrame(columns=['mrn', 'loinc_code'])
for index, row in df_patients.iterrows():
    df_social_determinants_sample = df_social_determinants.sample(n=random.randint(1, 5))
    df_social_determinants_sample['mrn'] = row['mrn']
    df_patient_social_determinants = df_patient_social_determinants.append(df_social_determinants_sample)

df_patient_social_determinants



########## INSERTING DUMMY DATA #########

#INSERT FAKE PATIENTS
insertQuery = "INSERT INTO patients (mrn, first_name, last_name, zip_code, dob, gender, contact_mobile, contact_home) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

for index, row in df_fake_patients.iterrows():
    db_gcp.execute(insertQuery, (row['mrn'], row['first_name'], row['last_name'], row['zip_code'], row['dob'], row['gender'], row['contact_mobile'], row['contact_home']))
    print("inserted row: ", index)

df_gcp = pd.read_sql_query("SELECT * FROM patients", db_gcp)
df_gcp



#INSERT ICD10 CODES FOR CONDITIONS
insertQuery = "INSERT INTO conditions (icd10_code, icd10_description) VALUES (%s, %s)"

startingRow = 0
for index, row in icd10codesShort_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    db_gcp.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row: ", index)
    if startingRow == 100:
        break

df_gcp = pd.read_sql_query("SELECT * FROM conditions", db_gcp)
df_gcp



#INSERT NDC CODES FOR MEDICATIONS
insertQuery = "INSERT INTO medications (ndc_code, ndc_description) VALUES (%s, %s)"

startingRow = 0
for index, row in ndc_codes_1k.iterrows():
    startingRow += 1
    db_gcp.execute(insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    print("inserted row: ", index)
    if startingRow == 100:
        break

df_gcp = pd.read_sql_query("SELECT * FROM medications", db_gcp)
df_gcp



#INSERT CPT CODES FOR TREATMENTS/PROCEDURES
insertQuery = "INSERT INTO treatment_procedures (cpt_code, cpt_code_desciption) VALUES (%s, %s)"

startingRow = 0 
for index, row in cpt_codes_1k.iterrows():
    startingRow += 1
    db_gcp.execute(insertQuery, (row['com.medigy.persist.reference.type.clincial.CPT.code'], row['label']))
    print("inserted row: ", index)
    if startingRow == 100:
        break

df_gcp = pd.read_sql_query("SELECT * FROM treatment_procedures", db_gcp)
df_gcp



#INSERT LOINC CODES FOR SOCIAL DETERMINANTS
insertQuery = "INSERT INTO social_determinants (loinc_code, loinc_description) VALUES (%s, %s)"

startingRow = 0
for index, row in loinc_codes_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    db_gcp.execute(insertQuery, (row['LOINC_NUM'], row['LONG_COMMON_NAME']))
    print("inserted row: ", index)
    if startingRow == 100:
        break

df_gcp = pd.read_sql_query("SELECT * FROM social_determinants", db_gcp)
df_gcp



#INSERT FAKE PATIENT MEDICATIONS
insertQuery = "INSERT INTO patient_medications (mrn, ndc_code) VALUES (%s, %s)"

for index, row in df_patient_medications.iterrows():
    db_gcp.execute(insertQuery, (row['mrn'], row['ndc_code']))
    print("inserted row: ", index)

df_gcp = pd.read_sql_query("SELECT * FROM patient_medications", db_gcp)
df_gcp



#INSERT FAKE PATIENT CONDITIONS
insertQuery = "INSERT INTO patient_conditions (mrn, icd10_code) VALUES (%s, %s)"

for index, row in df_patient_conditions.iterrows():
    db_gcp.execute(insertQuery, (row['mrn'], row['icd10_code']))
    print("inserted row: ", index)

df_gcp = pd.read_sql_query("SELECT * FROM patient_conditions", db_gcp)
df_gcp



#INSERT FAKE PATIENT TREATMENT/PROCEDURES
insertQuery = "INSERT INTO patient_treatment_procedures (mrn, cpt_code) VALUES (%s, %s)"

for index, row in df_patient_treatment_procedures.iterrows():
    db_gcp.execute(insertQuery, (row['mrn'], row['cpt_code']))
    print("inserted row: ", index)

df_gcp = pd.read_sql_query("SELECT * FROM patient_treatment_procedures", db_gcp)
df_gcp



#INSERT FAKE PATIENT SOCIAL DETERMINANTS
insertQuery = "INSERT INTO patient_social_determinants (mrn, loinc_code) VALUES (%s, %s)"

for index, row in df_patient_social_determinants.iterrows():
    db_gcp.execute(insertQuery, (row['mrn'], row['loinc_code']))
    print("inserted row: ", index)

df_gcp = pd.read_sql_query("SELECT * FROM patient_social_determinants", db_gcp)