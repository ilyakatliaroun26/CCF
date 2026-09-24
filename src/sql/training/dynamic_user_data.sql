WITH base AS (
    SELECT user_id, reference_date
    FROM credit_risk_playground.bp_od_ccf_training_snapshot
),

kyc_process as (

    select
        cr.user_id,
        cr.reference_date,
        min(kp.completed) as first_recent_kyc_completed_at,
        max(kp.completed) as most_recent_kyc_completed_at
    from kalium_processes as kp
    inner join base as cr
        on cr.user_id = kp.user_id
        and kp.completed <= cr.reference_date
        and kp.type = 'KYC'
        and kp.completed is not null
    group by 1, 2

),

product_membership_raw as (

    select
        user_id,
        timestamp 'EPOCH' + subscription_valid_from / 1000 * INTERVAL '1 SECOND' as valid_from,
        coalesce(
            timestamp 'EPOCH' + subscription_valid_until / 1000 * INTERVAL '1 SECOND',
            '2100-01-01 00:00:00.000000'::timestamp
        ) as valid_until,
        purpose || '_' || tier as membership_product
    from product_usermembership

),

product_membership as (
    select
        cr.user_id,
        cr.reference_date,
        prd.membership_product,
        row_number() over (partition by cr.user_id, cr.reference_date order by prd.valid_from desc, prd.valid_until desc, prd.membership_product asc) as rn
    from base as cr
    inner join product_membership_raw as prd
        on cr.user_id = prd.user_id
        and cr.reference_date >= prd.valid_from and cr.reference_date < prd.valid_until

),

login_info as (

    select
        cr.user_id,
        cr.reference_date,
        max(
            case when ld.login_date >= dateadd(MONTHS, -3, cr.reference_date)
            then ld.n_login
            end
        ) as max_daily_logins_3m,
        max(
            case when ld.login_date >= dateadd(MONTHS, -6, cr.reference_date)
            then ld.n_login
            end
        ) as max_daily_logins_6m,
        max(
            case when ld.login_date >= dateadd(MONTHS, -12, cr.reference_date)
            then ld.n_login
            end
        ) as max_daily_logins_12m,
        max(ld.n_login) as max_daily_logins
    from base as cr
    inner join dbt.zrh_login_day as ld
        on cr.user_id = ld.user_id
        and cr.reference_date >= ld.login_date
    group by 1, 2

),

users as (

    select
        id as user_id,
        timestamp 'EPOCH' + birth_date / 1000 * INTERVAL '1 SECOND' as birth_date,
        nationality::text as nationality,
        gender
    from etl_reporting.customer_users

)

select distinct
    master.user_id,
    master.reference_date,
    u.gender,
    u.nationality,
    datediff(YEARS, u.birth_date, master.reference_date) as sd__age,
    datediff(DAYS, kyc.first_recent_kyc_completed_at::date, master.reference_date) as sd__age_since_first_kyc,
    datediff(DAYS, kyc.most_recent_kyc_completed_at::date, master.reference_date) as sd__age_since_last_kyc,
    pm.membership_product as sd__membership_product,
    li.max_daily_logins_6m as sd__max_daily_logins_6m,
    li.max_daily_logins_12m as sd__max_daily_logins_12m
from base as master
left join kyc_process as kyc
    on kyc.user_id = master.user_id
    and kyc.reference_date = master.reference_date
left join product_membership as pm
    on master.user_id = pm.user_id
    and master.reference_date = pm.reference_date
    and pm.rn = 1
left join login_info as li
    on li.user_id = master.user_id
    and li.reference_date = master.reference_date
left join users as u
    on master.user_id = u.user_id
