with base as (
    select user_id, reference_date from credit_risk_playground.bp_od_ccf_training_snapshot
)

-- Map Banken V3 SCHUFA ingested values to previous masterscale
, mapped_schufa_scores as (

    select 
        user_id,
        requested_on,
        provider,

        -- provisions logic incorporated to map following ratings to M
        case 
            when rating in ('H', 'I', 'K', 'L', 'UNKNOWN') then 'M'

            -- SCHUFA masterscale update to Banken_v3
            when requested_on >= '2025-07-05 21:57:00.000000'::timestamp then
                case
                    when rating = 'A' then 'B'
                    when rating = 'B' then 'C'
                    when rating = 'C' then 'D'
                    when rating = 'D' then 'E'
                    when rating = 'E' then 'F'
                    when rating = 'F' then 'G'
                    else rating
                end

            else rating
        end as rating

    from private.californium_credit_score_audit_log
),


ordered_schufa_scores as (

    select
        base.user_id,
        base.reference_date,
        ca.provider,
        ca.rating,
        ca.requested_on,
        row_number() over (partition by base.user_id, base.reference_date order by ca.requested_on desc) = 1 as last_value,
        row_number() over (partition by base.user_id, base.reference_date order by ca.rating desc) = 1 as worst_value
    from base
    inner join mapped_schufa_scores as ca
        on base.user_id = ca.user_id
        and ca.requested_on::date <= base.reference_date::date

),

most_recent_score as (

    select
        user_id,
        reference_date,
        provider,
        rating as most_recent_score
    from ordered_schufa_scores
    where last_value

),

worst_score as (

    select
        user_id,
        reference_date,
        provider,
        rating as worst_score
    from ordered_schufa_scores
    where worst_value

)

select
    base.user_id
    , base.reference_date
    , ms.most_recent_score as cb__most_recent_score
    , ws.worst_score as cb__worst_score
from base
left join most_recent_score as ms
    on base.user_id = ms.user_id
    and base.reference_date = ms.reference_date
left join worst_score as ws
    on base.user_id = ws.user_id
    and base.reference_date = ws.reference_date