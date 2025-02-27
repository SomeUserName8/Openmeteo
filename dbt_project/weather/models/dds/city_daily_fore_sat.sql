{{ config(
    materialized="table",
    schema="dds"
) }}

SELECT 
    distinct md5(cm.city) as city_id,
    json_array_elements_text(("data"->'daily')->'time')::date as date,
    json_array_elements_text(("data"->'daily')->'temperature_2m_max')::numeric as temperature_2m_max,
    json_array_elements_text(("data"->'daily')->'temperature_2m_min')::numeric as temperature_2m_min,
    json_array_elements_text(("data"->'daily')->'sunrise')::timestamp as sunrise,
    json_array_elements_text(("data"->'daily')->'sunset')::timestamp as sunset,
    json_array_elements_text(("data"->'daily')->'daylight_duration')::numeric as daylight_duration,
    json_array_elements_text(("data"->'daily')->'sunshine_duration')::numeric as sunshine_duration,
    json_array_elements_text(("data"->'daily')->'precipitation_sum')::numeric as precipitation_sum,
    json_array_elements_text(("data"->'daily')->'rain_sum')::numeric as rain_sum,
    json_array_elements_text(("data"->'daily')->'snowfall_sum')::numeric as snowfall_sum,
    json_array_elements_text(("data"->'daily')->'wind_speed_10m_max')::numeric as wind_speed_10m_max,
    now() as upload_date, 'API Open-Meteo' as source
from
    stg.forecast_data fd
left join stg.city_mapping cm on (
        (fd.data ->> 'latitude') :: numeric = cm.latitude
        and (fd.data ->> 'longitude'):: numeric = cm.longitude)
where fd.upload_date = (SELECT max(upload_date) from stg.forecast_data)