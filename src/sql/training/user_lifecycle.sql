with base as (
    select user_id, reference_date from credit_risk_playground.bp_od_ccf_training_snapshot
)

select 
    base.user_id
    , base.reference_date
    , case when zu.is_expat = true then 1 else 0 end as ul__is_expat
from base
left join dbt.zrh_users zu on base.user_id = zu.user_id
