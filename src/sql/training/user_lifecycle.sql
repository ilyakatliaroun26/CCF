with base as (
    select user_id, reference_date from credit_risk_playground.bp_od_ccf_training_snapshot
)

select 
    base.user_id
    , base.reference_date
    , case when zu.is_expat = true then 1 else 0 end as ul__is_expat
    , case when zu.country_tnc = 'DEU' then 1 else 0 end as ul__is_deu
    , case when zu.country_tnc = 'AUT' then 1 else 0 end as ul__is_aut
    , date_diff('month', zu.account_opened_at::date, base.reference_date::date) as ul__account_age_months
from base
left join dbt.zrh_users zu on base.user_id = zu.user_id
