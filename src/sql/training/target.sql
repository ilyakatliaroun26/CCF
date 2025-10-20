with pu_first_row as (
    select
        user_id,
        min(created) as min_rev_timestamp
    from public.pu_overdraft_history
    group by 1
),

appended as (
    -- Include rows from Plutonium after the first populated date
    select
        osa.user_id,
        osa.created as rev_timestamp,
        coalesce(
            lead(
                rev_timestamp - interval '0.000001 second', 1
            ) over (partition by osa.user_id order by rev_timestamp),
            '2100-01-01'
        )::timestamp as end_timestamp,
        case when osa.status = 'ENABLED' then 1 else 0 end as enabled,
        osa.amount_cents * 100 as max_amount_cents,
        osa.user_amount_cents * 100 as user_amount_cents,
        osa.potential_amount_cents * 100 as potential_amount_cents,
    from public.pu_overdraft_history as osa
    inner join pu_first_row as pfr using (user_id)
    where 1=1
    and rev_timestamp >= pfr.min_rev_timestamp
	-- todo: filter more detailed for migration timestamp

    union all

    -- Include rows from DDB before the first Plutonium populated date
    select
        u.id as user_id,
        osa.rev_timestamp,
        osa.end_timestamp,
        osa.enabled,
        osa.max_amount_cents,
        osa.user_amount_cents,
        osa.potential_max_amount_cents as potential_amount_cents,
    from public.ddb_overdraft_settings_aud as osa
    inner join etl_reporting.cmd_users as u using (user_created)
    left join pu_first_row as pfr
        on u.id = pfr.user_id
    where
        osa.rev_timestamp < pfr.min_rev_timestamp -- Include only historical records before Plutonium migration
),

lag_table as (
    select
        *,
        lead(rev_timestamp) over (partition by user_id order by rev_timestamp) as next_time_stamp,
        row_number() over (partition by user_id order by rev_timestamp) as aud_order
    from appended
)

select
    user_id,
    rev_timestamp,
    case when next_time_stamp is null then end_timestamp
        else least(end_timestamp, next_time_stamp) end as end_timestamp,
    enabled,
    max_amount_cents,
    user_amount_cents,
    potential_amount_cents
from lag_table
where rev_timestamp <= end_timestamp