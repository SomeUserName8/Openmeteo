create schema stg;

drop table stg.historical_data;
create table stg.historical_data(
upload_date timestamp not null,
data JSON not null);

drop table stg.forecast_data;
create table stg.forecast_data(
upload_date timestamp not null,
data JSON not null);

