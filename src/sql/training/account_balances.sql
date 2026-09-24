with base as (
    select user_id, reference_date from credit_risk_playground.bp_od_ccf_training_snapshot
),

daily_account_balances as (
    select 
        b.user_id
        , b.reference_date
        , db.date
        , sum(db.balance_eur) as balance_eur
        , lag(sum(db.balance_eur)) over (
            partition by b.user_id, b.reference_date 
            order by db.date
        ) as prev_balance_eur
    from base b
    left join dbt.mmb_daily_balance_aud db
        on b.user_id = db.user_id
        and db.date::date <= b.reference_date::date
    group by 1, 2, 3
),

features as (
    select
        user_id,
        reference_date,

        -- Average balance
        avg(case when date >= reference_date - interval '90 day' then balance_eur end) as ab__avg_bal_3m,
        avg(case when date >= reference_date - interval '30 day' then balance_eur end) - 
        avg(case when date < reference_date - interval '60 day' and date >= reference_date - interval '90 day' then balance_eur end) as ab__avg_bal_change_3m,

        -- Max balance
        max(case when date >= reference_date - interval '90 day' then balance_eur end) as ab__max_util_3m,
        max(case when date >= reference_date - interval '30 day' then balance_eur end) - 
        max(case when date < reference_date - interval '60 day' and date >= reference_date - interval '90 day' then balance_eur end) as ab__max_bal_change_3m,

        -- Volatility
        stddev(case when date >= reference_date - interval '90 day' then balance_eur end) as ab__std_util_3m,
        stddev(case when date >= reference_date - interval '30 day' then balance_eur end) - 
        stddev(case when date < reference_date - interval '60 day' and date >= reference_date - interval '90 day' then balance_eur end) as ab__std_bal_change_3m,


        -- Balance change
        case when (max(case when date >= reference_date - interval '90 day' then balance_eur end) - 
                   min(case when date >= reference_date - interval '90 day' then balance_eur end)) <> 0
             then 1 else 0 end as ab__bal_changed_3m,
        
        sum(case when date >= reference_date - interval '90 day' and balance_eur <> prev_balance_eur
                 then 1 else 0 end) as ab__bal_change_freq_3m,
        
        -- Volatility / shocks
        avg(case when date >= reference_date - interval '180 day' then abs(balance_eur - prev_balance_eur) else null end) as ab__mean_abs_change_6m,
        stddev(case when date >= reference_date - interval '180 day' then abs(balance_eur - prev_balance_eur) else null end) as ab__std_abs_change_6m,
        min(case when date >= reference_date - interval '180 day' then balance_eur - prev_balance_eur else null end) as ab__max_one_day_drop_6m,

        -- Positive inflows (income spikes)
        sum(case when date >= reference_date - interval '180 day' and (balance_eur - prev_balance_eur) > 200 then 1 else 0 end) as ab__inflow_spike_count_6m,

        -- Days balance decreased
        sum(case when date >= reference_date - interval '180 day' and balance_eur < prev_balance_eur then 1 else 0 end) as ab__days_down_6m,

        -- Days with zero or near-zero balance
        sum(case when date >= reference_date - interval '180 day' and balance_eur between -30 and 30 then 1 else 0 end) as ab__near_zero_days_6m
    
    from daily_account_balances
    group by 1,2   
)


select * from features