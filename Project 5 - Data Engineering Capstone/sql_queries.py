# DROP TABLES

immigration_table_drop = "DROP TABLE IF EXISTS fact_immigration;"
temperature_table_drop = "DROP TABLE IF EXISTS dim_temperature;"
demographics_table_drop = "DROP TABLE IF EXISTS dim_demographics;"
airport_table_drop = "DROP TABLE IF EXISTS dim_airport;"
country_code_table_drop = "DROP TABLE IF EXISTS country_code;"
state_code_table_drop = "DROP TABLE IF EXISTS state_code;"
port_infor_table_drop = "DROP TABLE IF EXISTS port_infor;"


# CREATE TABLES

immigration_table_create = ("""CREATE TABLE IF NOT EXISTS fact_immigration (
                                    cic_id bigint PRIMARY KEY,
                                    year int,
                                    month int,
                                    citizens_country_code int4,
                                    residential_country_code int4,
                                    port_code varchar,
                                    arrive_date date,
                                    mode int,
                                    address varchar,
                                    departure_date date,
                                    visa int,
                                    visa_post varchar,
                                    birth_year int4,
                                    gender varchar,
                                    airline varchar,
                                    admin_num bigint,
                                    flight_number varchar,
                                    visa_type varchar
                                    );
""")

temperature_table_create = ("""CREATE TABLE IF NOT EXISTS dim_temperature (
                                    dt date PRIMARY KEY,
                                    avg_temperature float,
                                    avg_temperature_uncertainty float,
                                    city varchar,
                                    country varchar,
                                    latitude varchar,
                                    longitude varchar,
                                    year int,
                                    month int
                                    );
""")

demographics_table_create = ("""CREATE TABLE IF NOT EXISTS dim_demographics (
                                    city varchar PRIMARY KEY,
                                    state varchar,
                                    median_age float,
                                    male_population bigint,
                                    female_population bigint,
                                    total_population bigint,
                                    veterans_number bigint,
                                    foreign_born bigint,
                                    avg_household_size float,
                                    state_code varchar,
                                    race varchar,
                                    count bigint
                                    );
""")

airport_table_create = ("""CREATE TABLE IF NOT EXISTS dim_airport (
                                ident varchar PRIMARY KEY,
                                type varchar,
                                name varchar,
                                elevation_ft float,
                                continent varchar,
                                iso_country varchar,
                                iso_region varchar,
                                municipality varchar,
                                gps_code varchar,
                                iata_code varchar,
                                local_code varchar,
                                coordinates varchar
                                );
""")

country_code_table_create = ("""CREATE TABLE IF NOT EXISTS country_code (
                                    country_code int PRIMARY KEY,
                                    country varchar
                                    );
    """)

state_code_table_create = ("""CREATE TABLE IF NOT EXISTS state_code (
                                    state_code varchar PRIMARY KEY,
                                    state varchar
                                    );
""")

port_infor_table_create = ("""CREATE TABLE IF NOT EXISTS port_infor (
                                    port_code varchar PRIMARY KEY,
                                    city varchar,
                                    state_code varchar
                                    );
""")


# INSERT RECORDS

immigration_table_insert = ("""INSERT INTO fact_immigration (cic_id, year, month, citizens_country_code, residential_country_code, port_code, arrive_date, mode, address, departure_date, visa,  visa_post, birth_year, gender, airline, admin_num, flight_number, visa_type)  
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                                    ON CONFLICT (cic_id) DO NOTHING;
""")

temperature_table_insert = ("""INSERT INTO dim_temperature (dt, avg_temperature, avg_temperature_uncertainty, city, country, latitude, longitude, year, month) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
                                    ON CONFLICT (dt) DO NOTHING;
""")

demographics_table_insert = ("""INSERT INTO dim_demographics (city, state, median_age, male_population, female_population, total_population, veterans_number, foreign_born, avg_household_size, state_code, race, count) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                                    ON CONFLICT (city) DO NOTHING;
""")

airport_table_insert = ("""INSERT INTO dim_airport (ident, type, name, elevation_ft, continent, iso_country, iso_region, municipality, gps_code, iata_code, local_code, coordinates) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                                ON CONFLICT (ident) DO NOTHING;
""")

country_code_table_insert = ("""INSERT INTO country_code (country_code, country) 
                                    VALUES (%s, %s) 
                                    ON CONFLICT (country_code) DO NOTHING;
""")

state_code_table_insert = ("""INSERT INTO state_code (state_code, state) 
                                    VALUES (%s, %s) 
                                    ON CONFLICT (state_code) DO NOTHING;
""")

port_infor_table_insert = ("""INSERT INTO port_infor (port_code, city, state_code) 
                                    VALUES (%s, %s, %s) 
                                    ON CONFLICT (port_code) DO NOTHING;
""")


# QUERY LISTS

create_table_queries = [immigration_table_create, temperature_table_create, demographics_table_create, airport_table_create, country_code_table_create, state_code_table_create, port_infor_table_create]
drop_table_queries = [immigration_table_drop, temperature_table_drop, demographics_table_drop, airport_table_drop, country_code_table_drop, state_code_table_drop, port_infor_table_drop]


