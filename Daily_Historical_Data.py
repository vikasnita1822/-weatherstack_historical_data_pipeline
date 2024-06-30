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

todays_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

def fetch_weather_data(city, date):
    params = {
        'access_key': API_KEY,
        'query': city,
        'historical_date': date
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    return data

def save_to_database(conn, data, city):
    c = conn.cursor()
    for date, details in data['historical'].items():
        c.execute('''
            INSERT INTO weather (date, city, sunrise, sunset, moonrise, moonset, moon_phase, moon_illumination, mintemp, maxtemp, avgtemp, totalsnow, sunhour, uv_index, hourly)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (date, city, details['astro']['sunrise'], details['astro']['sunset'], details['astro']['moonrise'], details['astro']['moonset'], details['astro']['moon_phase'], details['astro']['moon_illumination'], details['mintemp'], details['maxtemp'], details['avgtemp'], details['totalsnow'], details['sunhour'], details['uv_index'], json.dumps(details['hourly'])))
    conn.commit()
    conn.close()

for city in tqdm(cities):
    data = fetch_weather_data(city, todays_date)
    save_to_database(conn, data, city)
