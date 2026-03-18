-- models/staging/stg_sensor_daily.sql
with source as (
    select * from {{ source('sensor_data', 'sensor_daily_stats') }}
)

select
    date,
    machine_status,
    count,
    anomaly_count,
    anomaly_rate,
    broken_count
from source
where date is not null