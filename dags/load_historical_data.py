
import json
import pendulum
from airflow.decorators import dag, task
import requests
import psycopg
from airflow.models import Variable
from airflow.hooks.base import BaseHook



@dag(
    schedule='@daily',
    start_date=pendulum.datetime(2025, 1, 27, tz="UTC"),
    catchup=True
)
def getHistoracalData():

    pg_conn = BaseHook.get_connection("postgres_default")

    @task()
    def getData(ds, logical_date):
        url = f"https://historical-forecast-api.open-meteo.com/v1/forecast?latitude=56.75&longitude=37.1875&start_date={ds}&end_date={ds}&hourly=temperature_2m,relative_humidity_2m,precipitation_probability,rain,snowfall,pressure_msl,surface_pressure,visibility,wind_speed_10m,wind_direction_10m&daily=temperature_2m_max,temperature_2m_min,sunrise,sunset,daylight_duration,sunshine_duration,precipitation_sum,rain_sum,snowfall_sum,wind_speed_10m_max&wind_speed_unit=ms&timezone=Europe%2FMoscow"
        response = requests.get(url).json()

        with psycopg.connect(f"dbname={pg_conn.schema} user={pg_conn.login} password={pg_conn.password} host={pg_conn.host} port={pg_conn.port}") as conn:
            with conn.cursor() as cur:
                params = (logical_date, json.dumps(response))
                cur.execute("INSERT INTO stg.historical_data(upload_date, data) VALUES (%s, %s)", params)



    getData(ds="{{ds}}", logical_date="{{ logical_date }}") 

getHistoracalData()
