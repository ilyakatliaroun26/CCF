select distinct 
user_id
, end_time as reference_date
, max_amount_eur as ob__limit_ref
, coalesce(outstanding_balance_eur, 0) as ob__balance_ref
, case when max_amount_eur < coalesce(outstanding_balance_eur, 0) then 0
else (max_amount_eur - coalesce(outstanding_balance_eur, 0)) / max_amount_eur end as ob__open_limit_ref
, coalesce(outstanding_balance_eur, 0) / max_amount_eur as ob__avg_util_ref
from dbt.bp_overdraft_users
where end_time::date = last_day(date_add('month', -1, last_day(getdate())))::date -- reference date to change
and timeframe = 'day'
and od_enabled_flag = true