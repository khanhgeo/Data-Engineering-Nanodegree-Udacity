import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *


# create functions to process data
def sas_date_transform(date):
    """
    Function to tranforms SAS date format into standard date format
    """   
    return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')

def process_immigration_data(cur, conn):
    """
    Process immigration data to get fact_immigration tables

    """
    print("Start processing immigration data")
    
    # read i94 immigration data
    fname = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    immigration_df = pd.read_sas(fname, 'sas7bdat', encoding="ISO-8859-1")

    # drop unwanted and rename columns
    immigration_df = immigration_df[['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94port', 'arrdate', 'i94mode', 'i94addr', \
                                    'depdate', 'i94visa', 'visapost', 'biryear', 'gender', 'airline', 'admnum', 'fltno', 'visatype']]

    immigration_df.columns = ['cic_id', 'year', 'month', 'citizens_country_code', 'residential_country_code', 'port_code', 'arrive_date', 'mode', 'address', \
                            'departure_date', 'visa', 'visa_post', 'birth_year', 'gender', 'airline', 'admin_num', 'flight_number', 'visa_type']
    immigration_df.dropna(inplace=True)

    # change sas date format to standard date format
    immigration_df['arrive_date'] = sas_date_transform(immigration_df['arrive_date']) 
    immigration_df['departure_date'] = sas_date_transform(immigration_df['departure_date']) 
    
    # insert data into database table
    print("Inserting immigration table")
    for index, row in immigration_df.iterrows():
        cur.execute(immigration_table_insert, list(row.values))
        conn.commit()

def process_temperature_data(cur, conn):
    """
    Process temperature data to get dim_temperature tables

    """
    print("Start processing temperature data")
    
    # read temperature data
    fname = '../../data2/GlobalLandTemperaturesByCity.csv'
    temperature_df = pd.read_csv(fname)

    # select only US data
    temperature_us_df = temperature_df[temperature_df['Country']=='United States'].reset_index(drop=True)
    # remove missing temperature values
    temperature_us_df.dropna(inplace=True)

    # add year and month columns
    temperature_us_df['dt'] = pd.to_datetime(temperature_us_df['dt'])
    temperature_us_df['year'] = temperature_us_df['dt'].dt.year
    temperature_us_df['month'] = temperature_us_df['dt'].dt.month

    # rename columns
    temperature_us_df.columns = ['dt', 'avg_temperature', 'avg_temperature_uncertainty', 'city', 'country', 'latitude', 'longitude', 'year', 'month']
    
    # insert data into database table
    print("Inserting temperature table")
    for index, row in temperature_us_df.iterrows():
        cur.execute(temperature_table_insert, list(row.values))
        conn.commit()

def process_demographics_data(cur, conn):
    """
    Process demographics data to get dim_demographics tables

    """
    print("Start processing demographics table")

    # read us cities demographics data
    demographics_df = pd.read_csv("us-cities-demographics.csv", sep=";")

    # drop unwanted and rename columns
    demographics_df.dropna(inplace=True)
    demographics_df.columns = ['city', 'state','median_age', 'male_population', 'female_population', 'total_population', \
                                'veterans_number', 'foreign_born', 'avg_household_size', 'state_code', 'race', 'count']

    # insert data into database table
    print("Inserting demographics table")
    for index, row in demographics_df.iterrows():
        cur.execute(demographics_table_insert, list(row.values))
        conn.commit()

def process_airport_data(cur, conn):
    """
    Process airport data to get dim_airport_codes tables

    """
    print("Start processing airport data")
    
    # read airport codes data
    airport_codes_df = pd.read_csv("airport-codes_csv.csv")
    airport_codes_df = airport_codes_df.replace({np.NaN: None})

    # insert data into database table
    print("Inserting airport table")
    for index, row in airport_codes_df.iterrows():
        cur.execute(airport_table_insert, list(row.values))
        conn.commit()

def process_sas_label_desctiption(cur, conn):
    """
    Parsing sas label desctiption file to get codes of country, city, state

    """ 
    print("Start processing sas label desctiption file")  

    # Read SAS text file
    with open("./I94_SAS_Labels_Descriptions.SAS") as f:
        content = f.readlines()    

    # Get country_code
    country_code = []
    for line in content[10:298]:
        line = line.split("=")
        code = line[0].strip()
        country = line[1].strip().strip("' ;")
        country_code.append([code, country])
        
    # Create country_code dataframe
    country_code_df = pd.DataFrame(country_code, columns=["country_code", "country"])

    # insert data into database table
    print("Inserting country_code table")
    for index, row in country_code_df.iterrows():
        cur.execute(country_code_table_insert, list(row.values))
        conn.commit()

    # Get state code
    state_code = []
    for line in content[981:1036]:
        line = line.split("=")
        code = line[0].strip().strip("','")
        state = line[1].strip().strip("' ;")
        state_code.append([code, state])

    # Create state_code dataframe
    state_code_df = pd.DataFrame(state_code, columns=['state_code', 'state'])
    
    # insert data into database table
    print("Inserting state_code table")
    for index, row in state_code_df.iterrows():
        cur.execute(state_code_table_insert, list(row.values))
        conn.commit()

    # Get port information
    port = []
    for line in content[302:962]:
        line = line.split("=")
        port_code = line[0].strip("\t").strip().strip("''")
        city = line[1].split(",")[0].strip().strip("''")
        state_code = line[1].split(",")[-1].strip().strip("''")
        port.append([port_code, city, state_code])   

    # Create port_code dataframe
    port_infor_df = pd.DataFrame(port, columns=["port_code", "city", "state_code"])

    # insert data into database table
    print("Inserting port_infor table")
    for index, row in port_infor_df.iterrows():
        cur.execute(port_infor_table_insert, list(row.values))
        conn.commit()


def main():
    """
    - Connects to the capstoneprojectdb 
    - Process all data then insert data to all designed tables
    """    
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=capstoneprojectdb user=student password=student")
    cur = conn.cursor()

    process_immigration_data(cur, conn)
    process_temperature_data(cur, conn)
    process_demographics_data(cur, conn)
    process_airport_data(cur, conn)
    process_sas_label_desctiption(cur, conn)
    
    print("ETL process completed")

    conn.close()


if __name__ == "__main__":
    main()