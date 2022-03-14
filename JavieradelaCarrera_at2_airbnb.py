########################################################
#   AT2 - Javiera de la Carrera - 13743354 
#########################################################

########################################################
#
#   Libreries 
#
#########################################################
import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException

import pandas as pd
import numpy as np

from urllib.request import Request, urlopen

from pandas.io.json import json_normalize

import pandas as pd
import numpy as np
import time 
import re
import ast
import json
import math
import requests as req
#import swifter
import sys

import gzip
import shutil

import sqlalchemy
import psycopg2
import glob


########################################################
#
#   DAG Settings 
#
#########################################################

from airflow import DAG

#Setting start date as yesterday starts the DAG immediately when it is (useful if we add schedule). 
#Also, utcnow() can be added to put one type of hour.

dag_default_args = {
    'owner': 'Javiera_dela_Carrera',
    'start_date': datetime.now() - timedelta(days=1),  
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5, #2
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}


dag = DAG(
    dag_id='airbnb_final_Javiera_de_la_Carrera',
    default_args=dag_default_args,
    schedule_interval="@once", #@monthly for constant repetitions
    catchup=False,  #to avoid run the dag again for past dates (it has to be false with datetime.now()
    max_active_runs=1,
    concurrency=5
)

#########################################################
#
#   Custom Logics for Operator 
#
#########################################################

#########################################################
#   General features
#########################################################

#columns to drop because they are not present in all CSVs
drop = ["summary","space","experiences_offered","notes","transit","access","interaction","house_rules","thumbnail_url","medium_url",
       "xl_picture_url","city","state","zipcode","market","smart_location","country_code","country","is_location_exact","bed_type",
       "square_feet","weekly_price","monthly_price","security_deposit","cleaning_fee","guests_included","extra_people",
       "jurisdiction_names","is_business_travel_ready","cancellation_policy","require_guest_profile_picture","require_guest_phone_verification","street","requires_license"]


#########################################################
#   Extract datasets
#########################################################

#NOT USED
# def extract_gz():
#     # listings=pd.read_csv(a)
#     # listing_clean = listings.drop(columns=drop_cols)
#     # G01=pd.read_csv(b)
#     # G01= G01[c]
#     # G02=pd.read_csv(e)
#     # G02= G02[c1]
#     # data=pd.read_csv(b)
#     # try to work: please rename all the gz files as listing + 0-11
#     for i in range(0,12):
#         with gzip.open('dags/listings'+str(i)+'.gz', 'rb') as f_in:
#             with open('dags/listings'+str(i)+'.csv', 'wb') as f_out:
#                 shutil.copyfileobj(f_in, f_out)
#     #return f_out
    
#read csv 
def extract_csv(c):
    data=pd.read_csv(c)
    return data

#########################################################
#   Insert raw data
#########################################################

#Listings: the table was first created and then the values are inserted from the csv
#on conflict do nothing to avoid duplicates

def extract_insert_data_list(path, **kwargs):
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    #conn = psycopg2.connect("host=localhost dbname=airflow user=airflow password=airflow")
    cur = conn_ps.cursor() 

    csvs = [file for file in glob.glob(path)]
    for i in csvs:
        listings_f = pd.read_csv(i).drop(labels = drop, axis =1,errors='ignore')
        listings_f["filename"] = i

        # create (col1,col2,...)
        df_columns = list(listings_f)
        columns = ",".join(df_columns)
        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        insert_sql = "INSERT INTO {} ({}) {} ON CONFLICT ON CONSTRAINT apartment_raw DO NOTHING".format("raw.apartments",columns,values)
        cur.executemany(insert_sql, listings_f.values)
        conn_ps.commit()

    return None

    
#Location: the table was first created and then the values are inserted from the csv
#on conflict do nothing to avoid duplicates

def extract_insert_loc(path,**kwargs):

    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    cur = conn_ps.cursor()

    location=pd.read_csv(path)

    df_columns = list(location)
        # create (col1,col2,...)
    columns = ",".join(df_columns)
        # create VALUES('%s', '%s",...) one '%s' per column
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
    insert_sql = "INSERT INTO {} ({}) {} ON CONFLICT ON CONSTRAINT location_raw DO NOTHING".format("raw.location",columns,values)

    cur.executemany(insert_sql, location.values)
    conn_ps.commit()

    return None  

#Census:  the table was created from the csv file (extract and load)
#replace to avoid duplicates

def extract_insert_census(pathg01,pathg02,**kwargs):
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    #conn_ps = ps_pg_hook.get_conn()
    engine = ps_pg_hook.get_sqlalchemy_engine()

    con = engine.connect()

    G01=pd.read_csv(pathg01)
    G02=pd.read_csv(pathg02)

    #replace values if find the table (useful if we want to add more columns to the table) 
    table_name = 'g01'
    G01.to_sql(table_name, con,if_exists='replace',schema='raw',index=False)

    table_name = 'g02'
    G02.to_sql(table_name, con,if_exists='replace',schema='raw',index=False)
 
    return None 


#########################################################
#
#   DAG Operator Setup
#
#########################################################

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


#########################################################
#   creating raw schema and tables 
#########################################################

# Star schema
#IF NOT EXISTS no avoid duplicate the schema and tables

create_raw_schema = PostgresOperator(
    task_id="create_raw_schema",
    postgres_conn_id="postgres",
    sql="""
        CREATE SCHEMA IF NOT EXISTS raw;
    """,
    dag=dag
)

# create raw location LGA table

create_table_location = PostgresOperator(
    task_id="create_table_location",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS raw.location (
        LOCALITY_ID varchar NULL,
        LOCALITY_NAME varchar NULL,
        LOCALITY_TYPE varchar NULL,
        POSTCODE decimal NULL,
        STATE varchar NULL,
        LGA_CODE varchar NULL,
        LGA_NAME varchar NULL,
        CONSTRAINT location_raw PRIMARY KEY (LOCALITY_NAME,LOCALITY_TYPE,POSTCODE,STATE)
        );
    """,
    dag=dag
)

# create raw listings table with the 74 variables that match between the files

create_table_listings = PostgresOperator(
    task_id="create_table_listings",
    postgres_conn_id="postgres",
    sql="""
CREATE TABLE IF NOT EXISTS raw.apartments (
            id numeric NOT NULL,
            listing_url varchar NULL,
            scrape_id varchar NULL,
            last_scraped varchar NULL,
            name varchar NULL,
            description text NULL,
            neighborhood_overview text NULL,
            picture_url varchar NULL,
            host_id numeric NOT NULL,
            host_url varchar NULL,
            host_name varchar NULL,
            host_since varchar NULL,
            host_location varchar NULL,
            host_about varchar NULL,
            host_response_time varchar NULL,
            host_response_rate varchar NULL,
            host_acceptance_rate varchar NULL,
            host_is_superhost varchar NULL,
            host_thumbnail_url varchar NULL,
            host_picture_url varchar NULL,
            host_neighbourhood varchar NULL,
            host_listings_count numeric NULL,
            host_total_listings_count numeric NULL,
            host_verifications text NULL,
            host_has_profile_pic varchar NULL,
            host_identity_verified varchar NULL,
            neighbourhood varchar NULL,
            neighbourhood_cleansed varchar NULL,
            neighbourhood_group_cleansed varchar NULL,
            latitude numeric NULL,
            longitude numeric NULL,
            property_type varchar NULL,
            room_type varchar NULL,
            accommodates numeric NULL,
            bathrooms varchar NULL,
            bathrooms_text varchar NULL,
            bedrooms numeric NULL,
            beds numeric NULL,
            amenities text NULL,
            price varchar NULL,
            minimum_nights numeric NULL,
            maximum_nights numeric NULL,
            minimum_minimum_nights numeric NULL,
            maximum_minimum_nights numeric NULL,
            minimum_maximum_nights numeric NULL,
            maximum_maximum_nights numeric NULL,
            minimum_nights_avg_ntm numeric NULL,
            maximum_nights_avg_ntm numeric NULL,
            calendar_updated varchar NULL,
            has_availability varchar NULL,
            availability_30 numeric NULL,
            availability_60 numeric NULL,
            availability_90 numeric NULL,
            availability_365 numeric NULL,
            calendar_last_scraped varchar NOT NULL,
            number_of_reviews numeric NULL,
            number_of_reviews_ltm numeric NULL,
            number_of_reviews_l30d numeric NULL,
            first_review varchar NULL,
            last_review varchar NULL,
            review_scores_rating numeric NULL,
            review_scores_accuracy numeric NULL,
            review_scores_cleanliness numeric NULL,
            review_scores_checkin numeric NULL,
            review_scores_communication numeric NULL,
            review_scores_location numeric NULL,
            review_scores_value numeric NULL,
            license varchar NULL,
            instant_bookable varchar NULL,
            calculated_host_listings_count numeric NULL,
            calculated_host_listings_count_entire_homes numeric NULL,
            calculated_host_listings_count_private_rooms numeric NULL,
            calculated_host_listings_count_shared_rooms numeric NULL,
            reviews_per_month numeric NULL,
            filename varchar NULL,
            CONSTRAINT apartment_raw PRIMARY KEY (id,filename)
            );
    """,
    dag=dag
)


#########################################################
#   inserting raw data in location, listings and census
#########################################################

#extract and load location_lga csv

# TO DO: change the path to the folder named "others" inside your dag folder
extract_insert_table_loc = PythonOperator(
    task_id="extract_insert_table_loc",
    python_callable=extract_insert_loc,
    op_kwargs={
        "path":'dags/others/location_lga.csv'
    },
    provide_context=True,
    dag=dag
)

#G01 and G02 (created and insert at the same time)

# TO DO: change the path to the folder named "others" inside your dag folder

create_insert_table_census = PythonOperator(
    task_id="create_insert_table_census",
    python_callable=extract_insert_census,
    op_kwargs={
        "pathg01":'dags/others/2016Census_G01_NSW_LGA.csv',
        "pathg02":'dags/others/2016Census_G02_NSW_LGA.csv'
    },
    provide_context=True,
    dag=dag
)

#extract and load listings csv for all months in the folder

# TO DO: change the path to the folder named "listings" inside your dag folder
extract_insert_table_listings = PythonOperator(
    task_id='extract_insert_table_listings',
    python_callable=extract_insert_data_list,
    op_kwargs={
        "path":'dags/listings/*.csv'
        },
    provide_context=True,
    dag=dag
)

#########################################################
#   Creating star schema 
#########################################################

#schema
#IF NOT EXISTS to avoid duplicate schema and tables

create_star_schema = PostgresOperator(
    task_id="create_star_schema",
    postgres_conn_id="postgres",
    sql="""
        CREATE SCHEMA IF NOT EXISTS star;
    """,
    dag=dag
)

## DIMENSIONS TABLES ##

#owners
create_star_owners = PostgresOperator(
    task_id="create_star_owners",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS star.owners (
            host_id numeric NOT NULL,
            host_name varchar NULL,
            host_loc_cleaned varchar NULL,
            host_total_listings_count numeric NULL,
            host_is_superhost varchar NULL,
            lga_code varchar NULL,
            host_tot_list_new numeric NULL,
            CONSTRAINT owners_pk PRIMARY KEY (host_id)
            );
    """,
    dag=dag
)

#census: choosing some variables from raw and creating some from G01
create_star_census = PostgresOperator(
    task_id="create_star_census",
    postgres_conn_id="postgres",
    sql="""
            CREATE TABLE IF NOT EXISTS star.census(
            LGA_CODE_2016 varchar NOT NULL,
            Tot_P_P numeric NULL,
            Australian_citizen_P numeric NULL,
            Indigenous_P_Tot_P numeric NULL,
            Tot_P_un55 numeric NULL,
            Tot_p_up55 numeric NULL,
            Median_age_persons numeric NULL,
            Median_mortgage_repay_monthly numeric NULL,
            Average_household_size numeric NULL,
            CONSTRAINT census_pk PRIMARY KEY (LGA_CODE_2016)
            );
    """,
    dag=dag
)


## FACT TABLE##

#apartments 
# lga_code could be null if some new apartment does not have a lga. We may not know its value at the time the data is entered
create_star_apartments = PostgresOperator(
    task_id="create_star_apartments",
    postgres_conn_id="postgres",
    sql="""
            CREATE TABLE IF NOT EXISTS star.apartments (
            id numeric NOT NULL,
            host_id numeric NOT NULL,
            neighbourhood_cleansed varchar NULL,
            price numeric NULL,
            room_type varchar NULL,
            property_type varchar NULL,
            accommodates numeric NULL,
            beds numeric NULL,
            minimum_nights numeric NULL,
            maximum_nights numeric NULL,
            availability_30 numeric NULL,
            has_availability varchar NULL,
            number_of_reviews numeric NULL,
            calendar_last_scraped varchar NOT NULL,
            month numeric NULL,
            year numeric NULL,
            lga_code varchar NULL,
            review_scores_rating numeric NULL,
            CONSTRAINT apartments_pk PRIMARY KEY (id,year,month),
            CONSTRAINT census_fk FOREIGN KEY (lga_code) REFERENCES star.census(LGA_CODE_2016),
            CONSTRAINT owners_fk FOREIGN KEY (host_id) REFERENCES star.owners(host_id)
            );
    """,
    dag=dag
)
           

#########################################################
#   Inserting data into star schema
#########################################################

#Apartments

# some new variables were created like the month, the year from the filename, and lga from location_lga. Price was cleaned 
# all the conflict to primary keys were added
insert_star_apartment = PostgresOperator(
    task_id="insert_star_apartment",
    postgres_conn_id="postgres",
    sql="""
        INSERT INTO star.apartments (id, host_id,neighbourhood_cleansed, price, 
        room_type,property_type,accommodates,beds,minimum_nights,
        maximum_nights,availability_30,has_availability,number_of_reviews,
        calendar_last_scraped,month,year,review_scores_rating,lga_code)

        SELECT id, host_id,neighbourhood_cleansed, 
        (Case When raw.apartments.filename = 'dags/listings/2020-07-01.csv' Then cast(raw.apartments.price as numeric)
        Else split_part(raw.apartments.price,'$', 2)::money::numeric::float8
        End)  as price ,
        room_type,property_type,CAST(accommodates as numeric),beds,minimum_nights,
        maximum_nights,availability_30,has_availability,number_of_reviews, calendar_last_scraped, extract(month from cast(split_part(regexp_replace(raw.apartments.filename,'.+/', ''),'.',1) as date)),extract(year from cast(split_part(regexp_replace(raw.apartments.filename,'.+/', ''),'.',1) as date)),
        review_scores_rating,LGA_CODE
        FROM raw.apartments
        LEFT JOIN
        raw.location
        on UPPER(raw.apartments.neighbourhood_cleansed) = UPPER(raw.location.locality_name)
        where raw.apartments.id not IN (select id
	    from raw.apartments
	    group by id
	    HAVING count(distinct(neighbourhood_cleansed)) > 1)

        ON CONFLICT (id,year,month) DO NOTHING
    """,
    dag=dag
)

#Owners

# some new variables were created like the count of host listings, and lga from location_lga and host_location. 
# all the conflict to primary keys were added
insert_star_owners = PostgresOperator(
    task_id="insert_star_owners",
    postgres_conn_id="postgres",
    sql="""
    INSERT INTO star.owners (host_id,host_name,host_loc_cleaned,host_total_listings_count,host_is_superhost,lga_code,host_tot_list_new)
    
    SELECT raw.apartments.host_id,host_name, UPPER(split_part(raw.apartments.host_location,',', 1)),host_total_listings_count,host_is_superhost, lga_code, host_tot_list_new
    FROM raw.apartments
    LEFT JOIN
    raw.location
    ON
    TRIM(UPPER(split_part(raw.apartments.host_location,',', 1))) = TRIM(UPPER(raw.location.locality_name))
    left join 
    (select host_id as host_id, count(distinct(id)) as host_tot_list_new
	from raw.apartments
	where raw.apartments.id not IN (select id
	from raw.apartments
	group by id
	HAVING count(distinct(neighbourhood_cleansed)) > 1)
 	group by host_id) as m1
 	on 
 	raw.apartments.host_id=m1.host_id
    where raw.apartments.id not IN (select id
	from raw.apartments
	group by id
	HAVING count(distinct(neighbourhood_cleansed)) > 1) 

    ON CONFLICT (host_id) DO NOTHING
    """,
    dag=dag
)

#Census 

#Choosing some variables from raw ans creatind the total population under and over 55 years
insert_star_census = PostgresOperator(
    task_id="insert_star_census",
    postgres_conn_id="postgres",
    sql="""
        INSERT INTO star.census (LGA_CODE_2016, tot_p_p, Australian_citizen_P,Indigenous_P_Tot_P, Tot_P_un55, Tot_P_up55, median_age_persons, median_mortgage_repay_monthly,Average_household_size)
        SELECT raw.g01."LGA_CODE_2016", raw.g01."Tot_P_P",raw.g01."Australian_citizen_P",raw.g01."Indigenous_P_Tot_P",  (raw.g01."Age_0_4_yr_P" +raw.g01."Age_5_14_yr_P" +raw.g01."Age_15_19_yr_P" + raw.g01."Age_20_24_yr_P" + raw.g01."Age_25_34_yr_P" + raw.g01."Age_35_44_yr_P" + raw.g01."Age_45_54_yr_P") as sum,
            (raw.g01."Age_55_64_yr_P" +raw.g01."Age_65_74_yr_P"+ raw.g01."Age_75_84_yr_P" + raw.g01."Age_85ov_P") as sum1, raw.g02."Median_age_persons" , raw.g02."Median_mortgage_repay_monthly" , raw.g02."Average_household_size"
        FROM raw.g01
        INNER JOIN
        raw.g02
        ON
        raw.g01."LGA_CODE_2016" = raw.g02."LGA_CODE_2016"             
        ON CONFLICT (LGA_CODE_2016) DO nothing;
    """,
    dag=dag
)

#Changing null values in star.owners lga to "out of NSW"

insert_star_lga_owners = PostgresOperator(
    task_id="insert_star_lga_owners",
    postgres_conn_id="postgres",
    sql="""
        update star.owners 
        set lga_code = 'Out of NSW'
        WHERE star.owners.lga_code IS NULL  
    """,
    dag=dag
)

#########################################################
#   QS 3: KPIS 
#########################################################

#create KPI schema 

create_kpi_schema = PostgresOperator(
    task_id="create_kpi_schema",
    postgres_conn_id="postgres",
    sql="""
        CREATE SCHEMA IF NOT EXISTS kpi;
    """,
    dag=dag
)

#All KPI tables have a restriction "On conflict do nothing" to avoid duplicates

##########
#  KPI 1 #
##########

#table
create_kpi1_table = PostgresOperator(
    task_id="create_kpi1_table",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS kpi.neighbourhood_month (
        year varchar NULL,
        month varchar NULL,
        lga_code varchar NULL,
        Active_listings_rate varchar NULL,
        min_price numeric NULL,
        max_price numeric NULL,
        median_price numeric NULL,
        ave_price numeric NULL,
        distinct_hosts numeric NULL,
        Superhost_rate numeric NULL,
        Average_of_review_scores_rating numeric NULL,
        change_for_active_listings numeric NULL,
        change_for_inactive_listings numeric NULL,
        Number_of_stays_sum numeric NULL,
        Number_of_stays_avg numeric NULL,
        Estimated_revenue numeric NULL,
        CONSTRAINT kpi1_pk PRIMARY KEY (year,month,lga_code)
        );
    """,
    dag=dag
)

#insert into table KPI1
create_kpi1_insert = PostgresOperator(
    task_id="create_kpi1_insert",
    postgres_conn_id="postgres",
    sql="""
        INSERT INTO kpi.neighbourhood_month (year, month, lga_code,Active_listings_rate,min_price ,max_price ,
        median_price, ave_price, distinct_hosts, Superhost_rate, Average_of_review_scores_rating,
        change_for_active_listings, change_for_inactive_listings, Number_of_stays_sum, Number_of_stays_avg, Estimated_revenue)

        SELECT year,month, lga_code,(cast(m1.id as decimal)/cast(m2.id as decimal))*100 as actlist,
        m3.minprice,m3.maxprice, m3.medianprice, m3.avgprice,
        m4.hostid, 
        cast(m5.hostid1 as decimal)/cast(m4.hostid as decimal)*100 as superh,
        mm3.avgreview, 
        m7.percent_change, m8.percent_changef,
        m6.sum,m6.av, m6.revenue
        FROM (
        select count(id) as id,lga_code, month,year 
        from star.apartments 
        where has_availability = 't'
        group by lga_code, month,year
           ) m1
        join (
    	select year,month,lga_code,count(id) as id 
        from star.apartments 
        group by lga_code, month,year
          ) m2
        USING (lga_code, month,year)
        join (
    	select year,month,lga_code,min(price) as minprice, max(price) as maxprice, 
    	avg(price) as avgprice, percentile_cont(0.5) WITHIN GROUP (ORDER BY price) as medianprice
        from star.apartments 
        where has_availability = 't'
        group by lga_code, month,year) m3
        USING (lga_code, month,year)
        join (
    	select year,month,lga_code, AVG(review_scores_rating) as avgreview
        from star.apartments 
        where has_availability = 't' and review_scores_rating != 'NaN'
        group by lga_code, month,year) mm3
        USING (lga_code, month,year)
        join (
        select  count(DISTINCT host_id) as hostid, lga_code, month,year
        from star.apartments
        group by lga_code, month,year) m4
        using (lga_code, month,year)
        join (
        select count(DISTINCT a.host_id) as hostid1, x.lga_code, x.month,x.year
        from star.owners a
        join star.apartments x
        on x.host_id =a.host_id 
        where host_is_superhost = 't'
        group by x.lga_code, month,year) m5
        using (lga_code, month,year)
        join (
        select AVG(30 - availability_30) as av, SUM(30 - availability_30) as sum,  SUM((30 - (availability_30))*(price)) as revenue,  lga_code, month,year  
        from star.apartments
        where has_availability = 't'
        group by lga_code, month,year) m6
        using (lga_code, month,year)
        join(
        WITH x AS (
            SELECT count(id) as count1, month,year, lga_code
         , lag(count(id)) OVER (PARTITION BY lga_code ORDER BY lga_code,year,month) as last_amount
            FROM   star.apartments
            where has_availability = 't'
            group by year,month,lga_code
            )
        SELECT year,month, count1,lga_code,last_amount
            , (count1 - last_amount) AS abs_change
            , round((100 * (count1 - last_amount)) / (last_amount+ 1.0E-06), 2) AS percent_change
        FROM   x) m7
        using (lga_code, month,year)
        full outer join(
        WITH x AS (
            SELECT count(id) as count1, month,year, lga_code
           , lag(count(id)) OVER (PARTITION BY lga_code ORDER BY lga_code,year,month) as last_amount
            FROM   star.apartments
            where has_availability = 'f'
            group by year,month,lga_code
            )
        SELECT year,month, count1,lga_code,last_amount
          , (count1 - last_amount) AS abs_change
          , round((100 * (count1 - last_amount)) / (last_amount+ 1.0E-06), 2) AS percent_changef
        FROM   x) m8
        using (lga_code, month,year)

        ON CONFLICT (year,month,lga_code) DO NOTHING
        ;
    """,
    dag=dag
)

##########
#  KPI 2 #
##########

#table
create_kpi2_table = PostgresOperator(
    task_id="create_kpi2_table",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS kpi.apartments_amenities (
        year varchar NULL,
        month varchar NULL,
        property_type varchar NULL,
        room_type varchar NULL,
        accommodates varchar NULL,
        Active_listings_rate varchar NULL,
        min_price numeric NULL,
        max_price numeric NULL,
        median_price numeric NULL,
        ave_price numeric NULL,
        distinct_hosts numeric NULL,
        Superhost_rate numeric NULL,
        Average_of_review_scores_rating numeric NULL,
        change_for_active_listings numeric NULL,
        change_for_inactive_listings numeric NULL,
        Number_of_stays_sum numeric NULL,
        Number_of_stays_avg numeric NULL,
        Estimated_revenue numeric NULL,
        CONSTRAINT kpi2_pk PRIMARY KEY (property_type,room_type,accommodates,month,year)
        );
    """,
    dag=dag
)

#insert into table KPI2
create_kpi2_insert = PostgresOperator(
    task_id="create_kpi2_insert",
    postgres_conn_id="postgres",
    sql="""
        INSERT INTO kpi.apartments_amenities (year,month,property_type, room_type,accommodates,Active_listings_rate,
        min_price ,max_price ,median_price, ave_price, distinct_hosts, Superhost_rate, Average_of_review_scores_rating,
        change_for_active_listings, change_for_inactive_listings, Number_of_stays_sum, Number_of_stays_avg, Estimated_revenue)


        SELECT year,month, property_type, room_type, accommodates, (cast(m1.id as decimal)/cast(m2.id as decimal))*100 as actlist,
        m3.minprice,m3.maxprice, m3.medianprice, m3.avgprice,
        m4.hostid, 
        cast(m5.hostid1 as decimal)/cast(m4.hostid as decimal)*100 as superh,
        mm3.avgreview, 
        m7.percent_change, m8.percent_changef,
        m6.sum, m6.av, m6.revenue
        FROM (
        select count(id) as id,property_type,room_type,accommodates,month,year 
        from star.apartments 
        where has_availability = 't'
        group by property_type,room_type,accommodates,month,year
         ) m1
        full outer join (
	    select property_type,room_type,accommodates,count(id) as id,month,year 
        from star.apartments 
        group by property_type,room_type,accommodates,month,year
         ) m2
        USING (property_type,room_type,accommodates,month,year)
        join (
        select year,month,property_type,room_type,accommodates,min(price) as minprice, max(price) as maxprice,
	    avg(price) as avgprice, percentile_cont(0.5) WITHIN GROUP (ORDER BY price) as medianprice
        from star.apartments 
        where has_availability = 't'
        group by property_type,room_type,accommodates,month,year) m3
        USING (property_type,room_type,accommodates,month,year)
        join (
        select year,month,property_type,room_type,accommodates, AVG(review_scores_rating) as avgreview
        from star.apartments 
        where has_availability = 't' and review_scores_rating != 'NaN' 
        group by property_type,room_type,accommodates,month,year) mm3
        USING (property_type,room_type,accommodates,month,year)
        join (
        select year,month,count(DISTINCT host_id) as hostid, property_type,room_type,accommodates
        from star.apartments
        group by property_type,room_type,accommodates,month,year) m4
        using (property_type,room_type,accommodates,month,year)
        join (
        select x.year,x.month,count(DISTINCT a.host_id) as hostid1, x.property_type,x.room_type,x.accommodates
        from star.owners a
        join star.apartments x
        on x.host_id =a.host_id 
        where host_is_superhost = 't'
        group by property_type,room_type,accommodates,month,year) m5
        using (property_type,room_type,accommodates,month,year)
        join (
        select year,month, AVG(30 - availability_30) as av, SUM(30 - availability_30) as sum, SUM((30 - (availability_30))*(price)) as revenue,  property_type,room_type,accommodates  
        from star.apartments
        where has_availability = 't'
        group by property_type,room_type,accommodates,month,year) m6
        using (property_type,room_type,accommodates,month,year)
        join(
        WITH x AS (
        SELECT count(id) as count1, property_type,room_type,accommodates,month,year
         , lag(count(id)) OVER (PARTITION BY property_type,room_type,accommodates ORDER BY property_type,room_type,accommodates,year,month) as last_amount
        FROM   star.apartments
        where has_availability = 't'
        group by property_type,room_type,accommodates,year,month
        )
        SELECT count1,property_type,room_type,accommodates,month,year,last_amount
        , (count1 - last_amount) AS abs_change
        , round((100 * (count1 - last_amount)) / (last_amount+ 1.0E-06), 2) AS percent_change
        FROM   x) m7
        using (property_type,room_type,accommodates,month,year)
        full outer join(
        WITH x AS (
        SELECT count(id) as count1, property_type,room_type,accommodates,year, month
         , lag(count(id)) OVER (PARTITION BY property_type,room_type,accommodates ORDER BY property_type,room_type,accommodates,year,month) as last_amount
        FROM   star.apartments
         where has_availability = 'f'
        group by property_type,room_type,accommodates,year, month
        )
        SELECT count1,property_type,room_type,accommodates,year,month,last_amount
         , (count1 - last_amount) AS abs_change
         , round((100 * (count1 - last_amount)) / (last_amount+ 1.0E-06), 2) AS percent_changef
        FROM   x) m8
        using (property_type,room_type,accommodates,month,year)
        ON CONFLICT ON CONSTRAINT kpi2_pk DO NOTHING
        ;
    """,
    dag=dag
)

##########
#  KPI 3 #
##########

#table
create_kpi3_table = PostgresOperator(
    task_id="create_kpi3_table",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS kpi.host_month (
        year varchar NULL,
        month varchar NULL,
        host_lga varchar NULL,
        dist_host numeric NULL,
        Estimated_Revenue numeric NULL,
        Estimated_Revenue_host numeric NULL,
        CONSTRAINT kpi3_pk PRIMARY KEY (host_lga, month,year)

        );
    """,
    dag=dag
)

#insert into table KPI3
create_kpi3_insert = PostgresOperator(
    task_id="create_kpi3_insert",
    postgres_conn_id="postgres",
    sql="""
        INSERT INTO kpi.host_month (year, month,host_lga, dist_host,Estimated_Revenue,Estimated_Revenue_host)

        SELECT year,month,host_lga, m1.host_id as distinct_hosts,
        m2.revenue, m2.revenue/m1.host_id as revenue_d_host
        FROM (
        select count(distinct d.host_id) as host_id , d.lga_code as host_lga, r.month,r.year
        from star.apartments as r
        join star.owners as d
        on r.host_id=d.host_id
        group by host_lga, r.month,r.year
           ) m1
        full outer join (
        select  SUM((30 - (j.availability_30))*(j.price)) as revenue ,b.lga_code as host_lga, j.month,j.year 
        from star.apartments as j
        join star.owners as b
        on j.host_id = b.host_id
        where has_availability = 't'
        group by  host_lga,month,year) m2
        using (host_lga, month,year)
        
        ON CONFLICT ON CONSTRAINT kpi3_pk DO NOTHING
        ;
    """,
    dag=dag
)


#########################################################
#   Order of the functions
#########################################################

#pipeline one after the other (Not Used)
# create_raw_schema >> create_table_location >> create_table_listings >> create_insert_table_census >> extract_insert_table_loc >> extract_insert_table_listings >> create_star_schema >> create_star_owners  >> \
#     create_star_census >> create_star_apartments >> insert_star_census >> insert_star_owners >> insert_star_lga_owners >>  insert_star_apartment  >>  \
#         create_kpi_schema >> create_kpi1_table >> create_kpi1_insert >> create_kpi2_table >> create_kpi2_insert >> create_kpi3_table >> create_kpi3_insert

#pipeline considering parallel work (apartments could not work parallel to census and owners because it depends on them)
create_raw_schema >> create_table_location >> extract_insert_table_loc >> create_star_schema 
create_raw_schema >> create_table_listings  >> extract_insert_table_listings >> create_star_schema 
create_raw_schema >> create_insert_table_census >> create_star_schema 
create_star_schema >> create_star_owners  >> insert_star_owners >> insert_star_lga_owners >> create_kpi_schema 
create_star_schema >> create_star_census >> insert_star_census >> create_star_apartments  >>  insert_star_apartment   >> create_kpi_schema
create_kpi_schema >> create_kpi1_table >> create_kpi1_insert 
create_kpi_schema>> create_kpi2_table >> create_kpi2_insert 
create_kpi_schema>> create_kpi3_table >> create_kpi3_insert
