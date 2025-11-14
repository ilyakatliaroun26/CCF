with base as (
    select user_id, reference_date from credit_risk_playground.bp_od_ccf_training_snapshot
)

, consumer_credit as (
    select
    laa.user_id,
    dcd.start_time,
    sum(principal_balance + interest_balance + interest_from_arrears_balance + fees_balance + penalty_balance) as cc_outstanding_balance_eur
from dbt.mmbr_loan_account_aud laa
inner join dbt.mmbr_loan_product_mapping lpm
    on laa.loan_name = lpm.loan_name
    and lpm.product in ('consumer_credit')
    inner join base b
        on laa.user_id = b.user_id
    inner join dwh_cohort_dates dcd
        on dcd.start_time between laa.rev_timestamp and laa.end_timestamp
        and dcd.start_time = b.reference_date
    group by laa.user_id, dcd.start_time
)

, tbil as (
    select
    laa.user_id,
    dcd.start_time,
    sum(principal_balance + interest_balance + interest_from_arrears_balance + fees_balance + penalty_balance) as tbil_outstanding_balance_eur
from dbt.mmbr_loan_account_aud laa
inner join dbt.mmbr_loan_product_mapping lpm
    on laa.loan_name = lpm.loan_name
    and lpm.product in ('installment_loans')
    inner join base b
        on laa.user_id = b.user_id
    inner join dwh_cohort_dates dcd
        on dcd.start_time between laa.rev_timestamp and laa.end_timestamp
        and dcd.start_time = b.reference_date
    group by laa.user_id, dcd.start_time
)

select 
base.user_id
, base.reference_date
, case when coalesce(cc.cc_outstanding_balance_eur, 0) > 0 and coalesce(t.tbil_outstanding_balance_eur, 0) > 0 then 1 else 0 end as ep__is_cc_tbil
, case when coalesce(cc.cc_outstanding_balance_eur, 0) > 0 then 1 else 0 end as ep__is_cc
, case when coalesce(t.tbil_outstanding_balance_eur, 0) > 0 then 1 else 0 end as ep__is_tbil
, cc.cc_outstanding_balance_eur as ep__cc_balance
, t.tbil_outstanding_balance_eur as ep__tbil_balance
from base
left join consumer_credit cc on cc.user_id = base.user_id
left join tbil t on t.user_id = base.user_id