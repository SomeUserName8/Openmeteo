{{ config(
    materialized="table",
    schema="dds"
) }}

select
    distinct md5(cm.city) as city_id, cm.city, cm.latitude, cm.longitude, now() as upload_date, 'API Open-Meteo' as source
from
    stg.historical_data hd
    left join stg.city_mapping cm on (
        (hd.data ->> 'latitude') :: numeric = cm.latitude
        and (hd.data ->> 'longitude'):: numeric = cm.longitude
)