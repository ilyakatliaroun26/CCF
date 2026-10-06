with base as (
    select distinct 
    user_id, 
    end_time as reference_date
    from dbt.bp_overdraft_users
    where end_time::date = last_day(date_add('month', -1, last_day(getdate())))::date -- reference date to change
    and timeframe = 'day'
    and od_enabled_flag = true
)

select 
    base.user_id
    , base.reference_date
    , case when zu.is_expat = true then 1 else 0 end as ul__is_expat
from base
left join dbt.zrh_users zu on base.user_id = zu.user_id