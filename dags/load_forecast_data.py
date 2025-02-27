
import json
import pendulum
from airflow.decorators import dag, task
import requests
import psycopg
from airflow.hooks.base import BaseHook
from airflow.macros import ds_add
from airflow.operators.bash import BashOperator


@dag(
    schedule='@daily',
    start_date=pendulum.datetime(2025, 2, 3, tz="UTC"),
    catchup=False
)
def getForecastData():

    pg_conn = BaseHook.get_connection("postgres_default")

    @task()
    def getData(ds, logical_date):
        ds_plus_day = ds_add(ds, 1)
        ds_plus_week = ds_add(ds, 8)
        url = f"https://api.open-meteo.com/v1/forecast?latitude=56.75&longitude=37.1875&start_date={ds_plus_day}&end_date={ds_plus_week}&hourly=temperature_2m,relative_humidity_2m,precipitation_probability,rain,snowfall,pressure_msl,surface_pressure,visibility,wind_speed_10m,wind_direction_10m&daily=temperature_2m_max,temperature_2m_min,sunrise,sunset,daylight_duration,sunshine_duration,precipitation_sum,rain_sum,snowfall_sum,wind_speed_10m_max&wind_speed_unit=ms&timezone=Europe%2FMoscow"
        response = requests.get(url).json()

        with psycopg.connect(f"dbname={pg_conn.schema} user={pg_conn.login} password={pg_conn.password} host={pg_conn.host} port={pg_conn.port}") as conn:
            with conn.cursor() as cur:
                params = (logical_date, json.dumps(response))
                cur.execute("INSERT INTO stg.forecast_data(upload_date, data) VALUES (%s, %s)", params)
    
    dbt_run = BashOperator(task_id='dbt_run', bash_command = 'cd /opt/airflow/dbt_project/weather && dbt run')
    snapshot = BashOperator(task_id='snapshot', bash_command = 'cd /opt/airflow/dbt_project/weather && dbt snapshot')


    getData(ds="{{ds}}", logical_date = "{{logical_date}}") >> dbt_run >> snapshot 

getForecastData()
