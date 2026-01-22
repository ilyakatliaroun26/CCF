select user_id 
    , reference_date
    , case when creation_date::date <= '2023-01-12'::date then 1 else 0 end as mv__is_lisbon_v1
    , case when creation_date::date between '2023-01-13'::date and '2023-06-14'::date then 1 else 0 end as mv__is_lisbon_v2
    , case when creation_date::date between '2023-06-15'::date and '2023-09-26'::date then 1 else 0 end as mv__is_lisbon_v3
    , case when creation_date::date between '2023-09-27'::date and '2024-01-21'::date then 1 else 0 end as mv__is_lisbon_v4
    , case when creation_date::date between '2024-01-22'::date and '2024-07-22'::date then 1 else 0 end as mv__is_porto_v1
    , case when creation_date::date >= '2024-07-23'::date then 1 else 0 end as mv__is_porto_v2
    , case when reference_date::date between '2020-01-01'::date and '2021-12-31'::date then 1 else 0 end as ef__is_covid
from credit_risk_playground.bp_od_ccf_training_snapshot