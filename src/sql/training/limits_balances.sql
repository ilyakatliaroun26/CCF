with skeleton as (
    select
        base.*,
        (base.default_date::date - interval '365 days') as reference_date
    from credit_risk_playground.bp_ccf_training_snapshot base
),

rep_plan as (
select distinct user_id, 'RP_0' as product, min(creation_date::date) as creation_date
from credit_risk_playground.bp_manual_repayment_plan
group by 1, 2

union

select distinct user_id, 'RP_1' as product, created::date as creation_date
from plutonium_repayment_plan

union

select distinct mum.user_id, 'RP_2' as product, la.creation_date from mmbr_loan_account la
inner join dbt.mmbr_user_match mum on mum.mmbr_client_key = la.account_holder_key and mum.is_current = true
inner join dbt.mmbr_loan_product_mapping mp on la.loan_name = mp.loan_name and mp.product = 'repayment_plans'
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

od_users_enabled_limits as (select
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
),

overdrafts_with_rp AS (
SELECT
o.*
, rp.creation_date as rp_creation_date
, case when rp.user_id is not null then rp.product else 'OD' end as product
FROM overdrafts o
left join rep_plan rp on rp.user_id = o.user_id
),

reference_dates AS (
select s.user_id
, s.default_date::date
, s.default_reason
, s.reference_date::timestamp as skeleton_reference_date
, o.rp_creation_date
, o.product
, case when o.creation_date::timestamp <= s.reference_date::timestamp then s.reference_date::timestamp
    else o.creation_date::timestamp end as reference_date
from skeleton s
inner join overdrafts_with_rp o on s.user_id = o.user_id
),

model_group AS (
select
  user_id
, case when product = 'OD' and date_add('day', 365, reference_date) <= default_date::date then 'group_1'
       when product = 'OD' and date_add('day', 365, reference_date) > default_date::date then 'group_2'
       when product in ('RP_0', 'RP_1', 'RP_2') and date_add('day', 14, default_date::date) >= rp_creation_date::date then 'group_3'
       else 'exluded' end as group_label
from reference_dates
),

final as (
select rd.user_id
, rd.default_date
, rd.default_reason
, rd.reference_date::timestamp
, rd.rp_creation_date
, rd.product
, mg.group_label
, coalesce(NULLIF(el.max_amount_cents::float, 0), (uref.max_amount_cents::float / 100.0)::float, 0.0) as LIMIT
, coalesce(uref.outstanding_balance_eur, 0) as BALANCE
, case when mg.group_label in ('group_1', 'group_2') then coalesce(udef.outstanding_balance_eur, udefa.outstanding_balance_eur, 0)
       when mg.group_label = 'group_3' then coalesce(udefrp.outstanding_balance_eur, 0)
       else 0 end as EAD 
--, add principal balance as EAD here for RP_0, RP_1, RP_2 
--, ((exposure_at_default - balance) / ("limit" - balance)) as CCF
from reference_dates rd
inner join model_group mg on mg.user_id = rd.user_id
inner join od_users_enabled_limits el on el.user_id = rd.user_id 
    and rd.reference_date::timestamp between el.rev_timestamp::timestamp and el.end_timestamp::timestamp
left join dbt.bp_overdraft_users uref 
    on uref.user_id = el.user_id 
    and uref.end_time::date = rd.reference_date::date 
    and uref.od_enabled_flag = 1
    and uref.timeframe = 'day'
left join dbt.bp_overdraft_users udef
    on udef.user_id = el.user_id 
    and udef.end_time::date = rd.default_date::date
    and udef.od_enabled_flag = 1
    and udef.timeframe = 'day'
left join dbt.bp_overdraft_users udefa 
    on udefa.user_id = el.user_id 
    and udefa.end_time::date = date_add('day', -1, rd.default_date::date)
    and udefa.od_enabled_flag = 1
    and udefa.timeframe = 'day'
left join dbt.bp_overdraft_users udefrp 
    on udefrp.user_id = el.user_id 
    and udefrp.end_time::date = rd.default_date::date
    and udefrp.timeframe = 'day'
)

select *
, BALANCE/"LIMIT" as avg_utilization_0M
, case when EAD <= BALANCE then 0.0
       when "LIMIT" - BALANCE <= "LIMIT" * 0.05 then EAD / "LIMIT"
       else (EAD - BALANCE) / ( "LIMIT" - BALANCE) end as CCF
from final
where "LIMIT" != 0
--and rd.rp_creation_date >= rd.reference_date