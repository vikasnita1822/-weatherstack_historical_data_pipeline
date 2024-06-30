import requests
import json
import datetime
import psycopg2
from tqdm import tqdm

file_path = 'creds_template.json'
with open(file_path,'r') as f:
    config_data = json.load()

db_host = config_data['POSTGRES_HOST']
db_port = config_data['POSTGRES_PORT']
db_name = config_data['POSTGRES_DATABASE']
db_user = config_data['POSTGRES_USERNAME']
db_password = config_data['POSTGRES_PASSWORD']
       
conn = psycopg2.connect(
        database=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port = db_port
    )

API_KEY = config_data['weatherstack_api_key']
BASE_URL = 'http://api.weatherstack.com/historical'

cities = config_data['cities']

def date_range_list(start_date,end_date):
    start_date_obj = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    date_list = []
    current_date = start_date_obj
    while current_date <= end_date_obj:
        date_list.append(current_date.strftime('%Y-%m-%d'))
        current_date += datetime.timedelta(days=1)

    date_string = ';'.join(date_list)
    return date_string

start_date = config_data['start_date']
end_date = config_data['end_date']

def fetch_weather_data(city, date_list):
    params = {
        'access_key': API_KEY,
        'query': city,
        'historical_date':date_list
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    return data

def save_to_database(conn ,data, city):
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            date DATE,
            city VARCHAR(50),
            sunrise VARCHAR(10),
            sunset VARCHAR(10),
            moonrise VARCHAR(10),
            moonset VARCHAR(10),
            moon_phase VARCHAR(20),
            moon_illumination INT,
            mintemp FLOAT,
            maxtemp FLOAT,
            avgtemp FLOAT,
            totalsnow FLOAT,
            sunhour FLOAT,
            uv_index FLOAT,
            hourly JSONB
        )
    ''')
    for date, details in data['historical'].items():
        c.execute('''
            INSERT INTO weather (date, city, sunrise, sunset, moonrise, moonset, moon_phase, moon_illumination, mintemp, maxtemp, avgtemp, totalsnow, sunhour, uv_index, hourly)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (date, city, details['astro']['sunrise'], details['astro']['sunset'], details['astro']['moonrise'], details['astro']['moonset'], details['astro']['moon_phase'], details['astro']['moon_illumination'], details['mintemp'], details['maxtemp'], details['avgtemp'], details['totalsnow'], details['sunhour'], details['uv_index'], json.dumps(details['hourly'])))
    conn.commit()
    conn.close()


date_list = date_range_list(start_date,end_date)

for city in tqdm(cities):
    data = fetch_weather_data(city, date_list)
    save_to_database(conn, data, city)
