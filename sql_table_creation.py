#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
GCP_MYSQL_HOSTNAME = os.getenv("GCP_MYSQL_HOSTNAME")
GCP_MYSQL_USER = os.getenv("GCP_MYSQL_USERNAME")
GCP_MYSQL_PASSWORD = os.getenv("GCP_MYSQL_PASSWORD")
GCP_MYSQL_DATABASE = os.getenv("GCP_MYSQL_DATABASE")

connection_string_gcp = f'mysql+pymysql://{GCP_MYSQL_USER}:{GCP_MYSQL_PASSWORD}@{GCP_MYSQL_HOSTNAME}:3306/{GCP_MYSQL_DATABASE}'
db_gcp = create_engine(connection_string_gcp)

print(db_gcp.table_names())

create_patients = """
create table if not exists patients (
    id int auto_increment,
    mrn varchar(255) default null unique,
    first_name varchar(255) default null,
    last_name varchar(255) default null,
    zip_code varchar(255) default null,
    dob varchar(255) default null,
    gender varchar(255) default null,
    contact_mobile varchar(255) default null,
    contact_home varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

create_medications = """
create table if not exists medications (
    id int auto_increment,
    ndc_code varchar(255) default null unique,
    ndc_description varchar(255) default null,
    med_is_dangerous varchar(255) default null,
    PRIMARY KEY (id)
); 
"""

create_conditions = """
create table if not exists conditions (
    id int auto_increment,
    icd10_code varchar(255) default null unique,
    icd10_description varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""

create_treatment_procedures = """
create table if not exists treatment_procedures (
    id int auto_increment,
    cpt_code varchar(255) null unique,
    cpt_code_description varchar(255) default null,
    PRIMARY KEY (id)
)
"""
create_social_determinants = """
create table if not exists social_determinants (
    id int auto_increment,
    loinc_code varchar(255) null unique,
    loinc_description varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""
#CREATE TABLES WITH FOREIGN KEYS
create_patient_medications = """
create table if not exists patient_medications (
    id int auto_increment,
    mrn varchar(255) default null,
    ndc_code varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (ndc_code) REFERENCES medications(ndc_code) ON DELETE CASCADE
); 
"""

create_patient_treatment_procedures ="""
create table if not exists patient_treatment_procedures (
  id int auto_increment,
  mrn varchar(255) default null,
  cpt_code varchar(255) default null,
  PRIMARY KEY (id),
  FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
  FOREIGN KEY (cpt_code) REFERENCES treatment_procedures(cpt_code) ON DELETE CASCADE
  );
  """

create_patient_conditions = """
create table if not exists patient_conditions (
    id int auto_increment,
    mrn varchar(255) default null,
    icd10_code varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (icd10_code) REFERENCES conditions(icd10_code) ON DELETE CASCADE
); 
"""

create_patient_social_determinants = """
create table if not exists patient_social_determinants (
    id int auto_increment,
    mrn varchar(255) default null,
    loinc_code varchar(255) default null,
    PRIMARY KEY (id), 
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (loinc_code) REFERENCES social_determinants(loinc_code) ON DELETE CASCADE
); 
"""
db_gcp.execute(create_patients)
db_gcp.execute(create_medications)
db_gcp.execute(create_treatment_procedures)
db_gcp.execute(create_conditions)
db_gcp.execute(create_social_determinants)
db_gcp.execute(create_patient_medications)
db_gcp.execute(create_patient_treatment_procedures)
db_gcp.execute(create_patient_conditions)
db_gcp.execute(create_patient_social_determinants)

# get tables from db_gcp
gcp_tables = db_gcp.table_names()
