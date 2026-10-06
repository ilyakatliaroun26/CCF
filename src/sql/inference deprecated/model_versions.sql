with base as (
    select distinct 
    user_id, 
    end_time as reference_date
    from dbt.bp_overdraft_users
    where end_time::date = last_day(date_add('month', -1, last_day(getdate())))::date -- reference date to change
    and timeframe = 'day'
    and od_enabled_flag = true
),

pu_first_row as (
    select
        user_id,
        min(created) as min_rev_timestamp
    from pu_overdraft_history
    group by 1
),

od_users as (
    -- Include rows from Plutonium after the first populated date
    select
        osa.user_id,
        s.encoded_key as instrument_id,
        osa.created as rev_timestamp,
        coalesce(
            lead(rev_timestamp - interval '0.000001 second', 1) 
            over (partition by osa.user_id order by rev_timestamp), '2100-01-01')::timestamp as end_timestamp,
        case when osa.status = 'ENABLED' then 1 else 0 end as enabled,
        coalesce(osa.amount_cents, 0)::numeric as max_amount_cents
    from pu_overdraft_history as osa
    inner join pu_first_row as pfr using (user_id)
    left join dbt.mmbr_user_match cl on cl.user_id = osa.user_id
    left join mmbr_savings_account s on s.encoded_key = cl.encoded_key and s.account_type = 'CURRENT_ACCOUNT'
    where 1=1
    and rev_timestamp >= pfr.min_rev_timestamp
	-- todo: filter more detailed for migration timestamp

    union all

    -- Include rows from DDB before the first Plutonium populated date
    select
        u.id as user_id,
        s.encoded_key as instrument_id,
        osa.rev_timestamp as rev_timestamp,
        osa.end_timestamp as end_timestamp,
        osa.enabled,
        coalesce(osa.max_amount_cents, 0)::numeric as max_amount_cents
    from ddb_overdraft_settings_aud as osa
    inner join etl_reporting.cmd_users as u using (user_created)
    left join pu_first_row as pfr
        on u.id = pfr.user_id
    left join dbt.mmbr_user_match cl on cl.user_id = u.id
    left join mmbr_savings_account s on s.encoded_key = cl.encoded_key and s.account_type = 'CURRENT_ACCOUNT'
    where
        osa.rev_timestamp < pfr.min_rev_timestamp -- Include only historical records before Plutonium migration
),

lag_table as (
    select
        *,
        lead(rev_timestamp) over (partition by user_id order by rev_timestamp) as next_time_stamp
    from od_users
),

od_users_enabled_limits as (
select
    user_id,
    instrument_id,
    rev_timestamp,
    case when next_time_stamp is null then end_timestamp
        else least(end_timestamp, next_time_stamp) end as end_timestamp,
    enabled,
    case when enabled = 1 then (max_amount_cents / 100)::float
        else 0 end as max_amount_cents
from lag_table
where rev_timestamp <= end_timestamp
),

overdrafts AS (
SELECT
user_id
, instrument_id
, min(rev_timestamp::timestamp)::timestamp AS creation_date
FROM
od_users_enabled_limits
where enabled = 1
GROUP BY 1, 2
)

select base.user_id 
    , base.reference_date
    , case when creation_date::date <= '2023-01-12'::date then 1 else 0 end as mv__is_lisbon_v1
    , case when creation_date::date between '2023-01-13'::date and '2023-06-14'::date then 1 else 0 end as mv__is_lisbon_v2
    , case when creation_date::date between '2023-06-15'::date and '2023-09-26'::date then 1 else 0 end as mv__is_lisbon_v3
    , case when creation_date::date between '2023-09-27'::date and '2024-01-21'::date then 1 else 0 end as mv__is_lisbon_v4
    , case when creation_date::date between '2024-01-22'::date and '2024-07-22'::date then 1 else 0 end as mv__is_porto_v1
    , case when creation_date::date >= '2024-07-23'::date then 1 else 0 end as mv__is_porto_v2
    , case when reference_date::date between '2020-01-01'::date and '2021-12-31'::date then 1 else 0 end as ef__is_covid
from base
left join overdrafts o on o.user_id = base.user_id