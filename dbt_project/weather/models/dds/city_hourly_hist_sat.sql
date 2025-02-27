{{ config(
    materialized="table",
    schema="dds"
) }}

SELECT 
    distinct md5(cm.city) as city_id,
    json_array_elements_text(("data"->'hourly')->'time')::date as date,
    json_array_elements_text(("data"->'hourly')->'time')::timestamp as time,
    json_array_elements_text(("data"->'hourly')->'temperature_2m')::numeric as temperature_2m,
    json_array_elements_text(("data"->'hourly')->'relative_humidity_2m')::int as relative_humidity_2m,
    json_array_elements_text(("data"->'hourly')->'precipitation_probability')::int as precipitation_probability,
    json_array_elements_text(("data"->'hourly')->'rain')::numeric as rain,
    json_array_elements_text(("data"->'hourly')->'snowfall')::numeric as snowfall,
    json_array_elements_text(("data"->'hourly')->'pressure_msl')::numeric as pressure_msl,
    json_array_elements_text(("data"->'hourly')->'surface_pressure')::numeric as surface_pressure,
    json_array_elements_text(("data"->'hourly')->'visibility')::numeric as visibility,
    json_array_elements_text(("data"->'hourly')->'wind_speed_10m')::numeric as wind_speed_10m,
    json_array_elements_text(("data"->'hourly')->'wind_direction_10m')::numeric as wind_direction_10m,
    now() as upload_date, 'API Open-Meteo' as source
from
    stg.historical_data hd
left join stg.city_mapping cm on (
        (hd.data ->> 'latitude') :: numeric = cm.latitude
        and (hd.data ->> 'longitude'):: numeric = cm.longitude)