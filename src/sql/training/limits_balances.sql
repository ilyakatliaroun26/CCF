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
, case when rp.user_id is not null then rp.product else 'OD' end as product
, rp.creation_date as rp_creation_date
FROM overdrafts o
left join rep_plan rp on rp.user_id = o.user_id
),

rp_balances AS (
select
    user_id,
    dcd.end_time,
    sum(principal_balance + interest_balance + interest_from_arrears_balance + fees_balance + penalty_balance) as outstanding_balance_eur
from dbt.mmbr_loan_account_aud laa
inner join dbt.mmbr_loan_product_mapping lpm
    on laa.loan_name = lpm.loan_name
    and lpm.product in ('repayment_plans')
inner join dwh_cohort_dates dcd on dcd.start_time between laa.rev_timestamp and laa.end_timestamp
group by user_id, dcd.end_time
),

limit_increases AS (
    select
        user_id,
        rev_timestamp as increase_date,
        max_amount_cents,
        lag(max_amount_cents) over (partition by user_id order by rev_timestamp) as prev_limit
    from od_users_enabled_limits
),

first_limit_increase AS (
    select
        li.user_id,
        max(li.increase_date) as first_increase_date
    from limit_increases li
    inner join skeleton s on li.user_id = s.user_id
    where
        li.prev_limit is not null
        and li.max_amount_cents > li.prev_limit -- actual increase
        and li.increase_date between (s.default_date::date - interval '365 days') and s.default_date::date
    group by li.user_id
),

reference_dates AS (
select
    s.user_id,
    s.default_date::date,
    s.default_reason,
    s.reference_date::timestamp as skeleton_reference_date,
    o.rp_creation_date,
    case when o.rp_creation_date > s.default_date then 'OD' else o.product end as product,
    case 
        when fli.first_increase_date is not null 
             and fli.first_increase_date between 
                 (case when o.creation_date::timestamp <= s.reference_date::timestamp 
                       then s.reference_date::timestamp else o.creation_date::timestamp end)
                 and s.default_date::timestamp
            then fli.first_increase_date
        else case when o.creation_date::timestamp <= s.reference_date::timestamp 
                  then s.reference_date::timestamp else o.creation_date::timestamp end
    end as reference_date
from skeleton s
inner join overdrafts_with_rp o on s.user_id = o.user_id
left join first_limit_increase fli on s.user_id = fli.user_id
),

limits_balances as (
select rd.user_id
, rd.default_date
, rd.default_reason
, rd.reference_date::timestamp
, rd.rp_creation_date
, rd.product
, coalesce(NULLIF(el.max_amount_cents::float, 0), (uref.max_amount_cents::float / 100.0)::float, 0.0) as LIMIT
, coalesce(uref.outstanding_balance_eur, 0) as BALANCE
, case when product in ('OD', 'RP_0')
    then coalesce(udef.outstanding_balance_eur, udefa.outstanding_balance_eur, 0) 
       when product in ('RP_1', 'RP_2') and rd.rp_creation_date > rd.default_date
    then coalesce(udef.outstanding_balance_eur, udefa.outstanding_balance_eur, 0)  
       when product in ('RP_1', 'RP_2')
    then coalesce(rpb.outstanding_balance_eur, 0)
       else 0 end as EAD
--, add principal balance as EAD here for RP_0, RP_1, RP_2 
--, ((exposure_at_default - balance) / ("limit" - balance)) as CCF
from reference_dates rd
inner join od_users_enabled_limits el 
    on el.user_id = rd.user_id 
    and rd.reference_date::timestamp between el.rev_timestamp::timestamp and el.end_timestamp::timestamp
left join dbt.bp_overdraft_users uref 
    on uref.user_id = el.user_id 
    and uref.end_time::date = rd.reference_date::date 
    and uref.od_enabled_flag = 1
    and uref.timeframe = 'day'
left join dbt.bp_overdraft_users udef
    on udef.user_id = el.user_id 
    and udef.end_time::date = rd.default_date::date
    and udef.timeframe = 'day'
left join dbt.bp_overdraft_users udefa 
    on udefa.user_id = el.user_id 
    and udefa.end_time::date = date_add('day', -1, rd.default_date::date)
    and udefa.timeframe = 'day'
left join rp_balances rpb 
    on rpb.user_id = rd.user_id 
    and rpb.end_time::date = rd.default_date::date

)

, final as (
select user_id
, default_date
, default_reason
, reference_date
, rp_creation_date
, product
, row_number() OVER ( PARTITION BY user_id 
    ORDER BY
    CASE WHEN user_id = '4ba98231-70d8-42d3-b197-f1b4c5bbf8ea' AND "LIMIT" = 250 THEN 1 ELSE 2 END,
    rp_creation_date DESC ) AS rn
, "LIMIT"
, BALANCE
, EAD
, BALANCE/"LIMIT" as avg_utilization_0M
, case when EAD <= BALANCE then 0.0
       when "LIMIT" - BALANCE <= "LIMIT" * 0.05 then EAD / "LIMIT" --check for 10%
       else (EAD - BALANCE) / ( "LIMIT" - BALANCE) end as CCF
from limits_balances
where "LIMIT" != 0
and reference_date <= coalesce(rp_creation_date, '2100-01-01'::timestamp)
and user_id not in (
    '1d5124d0-c1df-440b-b506-d12d8dbfa861',
    'b586f1e5-67ec-4ab5-b640-485593713fc5',
    '7a7060a8-c55e-47d2-b524-9e7d720d6024',
    '919aaa26-1abb-4cdf-b0b6-77724671b630',
    '3c3b1b48-75f3-475f-97d2-51d3412bae09',
    'd632d120-2f3b-4a14-af36-4fe2036deb7a'
    ) --RP users with data issues (duplicates bug in Mambu)
)

select * from final 
WHERE rn = 1