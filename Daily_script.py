# Steps
# truncate the staging table
# Extract data from API 
# load the data to staging table (daily)
# append the staging to main table 
# repeat the process



# libraries
import requests
import json
import pandas as pd

# sql and date
from sqlalchemy import create_engine
import pymysql
from datetime import date, timedelta


def url_daily():
    # Date for filter
    today = date.today() 
    yesterday = today - timedelta(days=1)

    url_daily = 'https://api.covid19api.com/country/UK?from='+ str(yesterday)+ '&to=' + str(today)
    return url_daily


def url_overall_data():
    # url for overall data since 2020
    url_overall = 'https://api.covid19api.com/dayone/country/UK'
    return url_overall


def extract_data_api(url_path):

    # extract the data from API to json format
    
    request_data = requests.get(url_path)

    py_data = request_data.json()

    # extract key field from json file using listening comprehension ( using .get to deal with try and except)

    Country  = [r.get('Country') for r in py_data]
    CountryCode = [r.get('CountryCode') for r in py_data]
    Province = [r.get('Province') for r in py_data]
    City  = [r.get('City') for r in py_data]
    CityCode = [r.get('CityCode') for r in py_data]
    Confirmed = [r.get('Confirmed') for r in py_data]
    Deaths = [r.get('Deaths') for r in py_data]
    Recovered = [r.get('Recovered') for r in py_data]
    Active = [r.get('Active') for r in py_data]
    Date = [pd.to_datetime(r.get('Date').split('T')[0], format='%Y%m%d', errors='ignore') for r in py_data]


    # Create a Dataframe for key fields

    new_data = pd.DataFrame({ 'Country':Country,
    'CountryCode':CountryCode,
    'Province':Province,
    'City':City,
    'CityCode':CityCode,
    'Confirmed':Confirmed,
    'Deaths':Deaths,
    'Recovered':Recovered,
    'Active':Active,
    'Date':Date,
    })
    
    return new_data


## Class for mysql and connect to sql ( which has function that truncates, load data and append data)
class mysql:
    
    def __init__(self):
        mysql_connection = {
            "host": "host",
            "user": "user",
            "passwd": "Password",
            "db":"Schema",
            "charset":'utf8mb4' }
        self.conn =  pymysql.connect(**mysql_connection)
        self.cur  = self.conn.cursor()

    def __exit__(self):
        self.conn.commit()
        self.conn.close()
        
    def truncate_table(self,sql_filename):
        sql_query = """{}""".format(sql_filename)
        try:
            self.cur.execute(sql_query)
        except:
            pass       

    def load_data(self):
        sql = 'insert into Jas_Schema.covid_tb_stg (Country,CountryCode, Province, City, CityCode, Confirmed, Deaths, Recovered, Active, Date)  VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s )'
        df_list = extract_data_api(url_daily()).values.tolist()
        n =0
        for i in extract_data_api(url_daily()).iterrows():
            self.cur.execute(sql,df_list[n])
            n +=1
            
    def merge_table(self,sql_filename):
        sql_query = """{}""".format(sql_filename)
        try:
            self.cur.execute(sql_query)
        except:
            pass 

    def append_table(self,sql_filename):
        sql_query = """{}""".format(sql_filename)
        try:
            self.cur.execute(sql_query)
        except:
            pass  
			

#final_script
# truncate the staging table
# load the data to staging table (daily)
# append the staging to main table 
# repeat the process

t = mysql()
t.truncate_table("truncate table Jas_Schema.covid_tb_stg")
t.load_data()
t.append_table("""insert into Jas_Schema.covid_tb  (
select * from Jas_Schema.covid_tb_stg)""")

t.__exit__()