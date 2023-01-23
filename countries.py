import csv
import pandas as pd
import os
import snowflake.connector

conn = snowflake.connector.connect(
    user=os.environ['snowflakeuser'],
    password=os.environ['snowflakepass'],
    account=os.environ['snowflakeaccount'],
    warehouse='COMPUTE_WH',
    role='ACCOUNTADMIN',
    database='SANDBOX',
    schema ='PUBLIC'
    )
c = conn.cursor()

c.execute('DROP TABLE IF EXISTS countries;')

c.execute('''
    CREATE TABLE countries (
    name TEXT NOT NULL ,
    alpha TEXT NOT NULL ,
    code INTEGER NOT NULL ,
    region TEXT NOT NULL ,
    intermediate_region TEXT,
    PRIMARY KEY (code)
    );
    ''')

with open('countries.csv','r') as fin:
    dr = csv.DictReader(fin) 
    to_db = [(i['name'], i['alpha-3'], i['country-code'], i['region'], i['intermediate-region']) for i in dr]

c.executemany("INSERT INTO countries (NAME, ALPHA, CODE, REGION, INTERMEDIATE_REGION) VALUES (%s, %s, %s, %s, %s);", to_db)

conn.commit()
c.close()
