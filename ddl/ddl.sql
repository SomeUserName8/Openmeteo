create schema stg;

drop table if exists stg.historical_data;
create table IF NOT EXISTS stg.historical_data(
upload_date timestamp not null,
data JSON not null);

drop table if exists stg.forecast_data;
create table if IF NOT EXISTS stg.forecast_data(
upload_date timestamp not null,
data JSON not null);

