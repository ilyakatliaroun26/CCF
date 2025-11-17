with base as (
    select user_id, reference_date, creation_date from credit_risk_playground.bp_od_ccf_training_snapshot
),

daily_account_balances as (
    select 
        b.user_id
        , b.reference_date
        , db.date
        , sum(db.balance_eur) as balance_eur
    from base b
    left join dbt.mmb_daily_balance_aud db
        on b.user_id = db.user_id
        and db.date::date <= b.creation_date::date
    group by 1, 2, 3
),

daily_overdraft_balances as (
    select
        b.user_id,
        b.reference_date,
        u.end_time,
        coalesce(u.outstanding_balance_eur,0) as outstanding_balance_eur,
        u.max_amount_eur,
        coalesce(u.outstanding_balance_eur / u.max_amount_eur, 0) as utilization,
        lag(coalesce(u.outstanding_balance_eur / u.max_amount_eur, 0)) over (partition by u.user_id order by u.end_time::date) as prev_utilization,
        lag(coalesce(u.outstanding_balance_eur,0)) over (partition by u.user_id order by u.end_time::date) as prev_balance,
        case when coalesce(prev_utilization,0) > 0 and utilization = 0 then 1 else 0 end as is_full_repayment
    from base b
    left join dbt.bp_overdraft_users u
        on b.user_id = u.user_id
       and u.timeframe = 'day'
       and od_enabled_flag = true
       and u.end_time::date <= b.reference_date::date
),

accb_features as (
    select
        user_id,
        reference_date,

        -- Average balance
        avg(case when date >= reference_date - interval '30 day' then balance_eur end) as ob__avg_bal_0_1m,
        avg(case when date < reference_date - interval '30 day' and date >= reference_date - interval '60 day' then balance_eur end) as ob__avg_bal_1_2m,
        avg(case when date < reference_date - interval '60 day' and date >= reference_date - interval '180 day' then balance_eur end) as ob__avg_bal_2_6m,

        -- Max balance
        max(case when date >= reference_date - interval '30 day' then balance_eur end) as ob__max_util_0_1m,
        max(case when date < reference_date - interval '30 day' and date >= reference_date - interval '60 day' then balance_eur end) as ob__max_bal_1_2m,
        max(case when date < reference_date - interval '60 day' and date >= reference_date - interval '180 day' then balance_eur end) as ob__max_bal_2_6m,

        -- Volatility
        stddev(case when date >= reference_date - interval '30 day' then balance_eur end) as ob__std_util_0_1m,
        stddev(case when date < reference_date - interval '30 day' and date >= reference_date - interval '60 day' then balance_eur end) as ob__std_util_1_2m,
        stddev(case when date < reference_date - interval '60 day' and date >= reference_date - interval '180 day' then balance_eur end) as ob__std_util_2_6m,


        -- Balance change
        case when (max(case when date >= reference_date - interval '30 day' then balance_eur end) - min(case when date >= reference_date - interval '30 day' then balance_eur end)) <> 0
             then 1 else 0 end as ob__bal_changed_1m,
        case when (max(case when date >= reference_date - interval '90 day' then balance_eur end) - min(case when date >= reference_date - interval '90 day' then balance_eur end)) <> 0
             then 1 else 0 end as ob__bal_changed_3m,
        case when (max(case when date >= reference_date - interval '180 day' then balance_eur end) - min(case when date >= reference_date - interval '180 day' then balance_eur end)) <> 0
             then 1 else 0 end as ob__bal_changed_6m
    
    from daily_account_balances
    group by 1,2
    
),

odb_features as (
    select
        user_id,
        reference_date,

        count(case when end_time >= reference_date - interval '365 day' then 1 end) as od__history_length_days,

        -- Average utilization
        avg(case when end_time >= reference_date - interval '30 day' then utilization end) as ob__avg_util_0_1m,
        avg(case when end_time < reference_date - interval '30 day' and end_time >= reference_date - interval '60 day' then utilization end) as ob__avg_util_1_2m,
        avg(case when end_time < reference_date - interval '60 day' and end_time >= reference_date - interval '180 day' then utilization end) as ob__avg_util_2_6m,

        -- Active utilization days
        count(case when utilization > 0 and end_time >= reference_date - interval '30 day' then 1 end) as ob__active_days_0_1m,
        count(case when utilization > 0 and end_time >= reference_date - interval '90 day' then 1 end) as ob__active_days_0_3m,

        -- Max utilization
        max(case when end_time >= reference_date - interval '30 day' then utilization end) as ob__max_util_0_1m,
        max(case when end_time < reference_date - interval '30 day' and end_time >= reference_date - interval '60 day' then utilization end) as ob__max_util_1_2m,
        max(case when end_time < reference_date - interval '60 day' and end_time >= reference_date - interval '180 day' then utilization end) as ob__max_util_2_6m,

        -- Active high utilization days
        sum(case when end_time >= reference_date - interval '30 day' and utilization > 0.8 then 1 else 0 end) / 30.0 as ob__pct_days_util_gt_80_1m,
        sum(case when end_time >= reference_date - interval '90 day' and utilization > 0.8 then 1 else 0 end) / 90.0 as ob__pct_days_util_gt_80_3m,
        
        -- Volatility
        stddev(case when end_time >= reference_date - interval '30 day' then utilization end) as ob__std_util_0_1m,
        stddev(case when end_time < reference_date - interval '30 day' and end_time >= reference_date - interval '60 day' then utilization end) as ob__std_util_1_2m,
        stddev(case when end_time < reference_date - interval '60 day' and end_time >= reference_date - interval '180 day' then utilization end) as ob__std_util_2_6m,

        -- Over-limit days
        sum(case when end_time >= reference_date - interval '180 day' and outstanding_balance_eur > max_amount_eur then 1 else 0 end) as ob__overlimit_days_6m,

        -- Full repayment events (bounce to zero)
        max(case when end_time >= reference_date - interval '180 day' then is_full_repayment else 0 end) as ob__full_repay_6m,

        -- Max daily increase / limit
        max(case when end_time >= reference_date - interval '30 day' then greatest(outstanding_balance_eur - coalesce(prev_balance,0),0) / max_amount_eur end) as ob__max_daily_incr_0_1m,
        max(case when end_time >= reference_date - interval '90 day' then greatest(outstanding_balance_eur - coalesce(prev_balance,0),0) / max_amount_eur end) as ob__max_daily_incr_0_3m,
        max(case when end_time >= reference_date - interval '180 day' then greatest(outstanding_balance_eur - coalesce(prev_balance,0),0) / max_amount_eur end) as ob__max_daily_incr_0_6m,

        -- Number of significant daily increases in the past 1m, 3m, 6m
        sum(case when end_time >= reference_date - interval '30 day' and greatest(outstanding_balance_eur - coalesce(prev_balance,0),0)/max_amount_eur > 0.3 then 1 else 0 end) as ob__num_daily_incr_0_1m,
        sum(case when end_time >= reference_date - interval '90 day' and greatest(outstanding_balance_eur - coalesce(prev_balance,0),0)/max_amount_eur > 0.3 then 1 else 0 end) as ob__num_daily_incr_0_3m,
        sum(case when end_time >= reference_date - interval '180 day' and greatest(outstanding_balance_eur - coalesce(prev_balance,0),0)/max_amount_eur > 0.3 then 1 else 0 end) as ob__num_daily_incr_0_6m


    from daily_overdraft_balances
    group by 1,2
)

select 
base.user_id
, base.reference_date
, ob__avg_bal_0_1m
from base
left join accb_features af on base.user_id = af.user_id
left join odb_features of on base.user_id = of.user_id