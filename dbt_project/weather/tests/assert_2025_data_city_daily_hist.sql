select "date"
from {{ ref('city_daily_hist_sat') }}
where "date" < '01-01-2025'::date