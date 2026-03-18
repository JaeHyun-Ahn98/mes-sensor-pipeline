-- models/marts/mart_anomaly_summary.sql
with staging as (
    select * from {{ ref('stg_sensor_daily') }}
)

select
    date,
    sum(count)                                    as total_count,
    sum(broken_count)                             as total_broken,
    sum(case when machine_status = 'RECOVERING' 
        then count else 0 end)                    as recovering_count,
    round(sum(broken_count) * 100.0 
        / nullif(sum(count), 0), 4)               as broken_rate
from staging
group by date
order by date