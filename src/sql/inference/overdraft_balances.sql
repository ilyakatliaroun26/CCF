with base_temp as (
    select distinct 
    user_id, 
    end_time as reference_date
    from dbt.bp_overdraft_users
    where end_time::date = last_day(date_add('month', -1, last_day(getdate())))::date -- reference date to change
    and timeframe = 'day'
    and od_enabled_flag = true
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

od_users_enabled_limits as (
select
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

base as (
    select b.user_id 
    , b.reference_date
    , o.creation_date
from base_temp b
left join overdrafts o on o.user_id = b.user_id
),

calendar AS (
    SELECT
        b.user_id,
        b.reference_date,
        b.creation_date,
        d.end_time::date AS day
    FROM base b
    JOIN dwh_cohort_dates d
      ON d.end_time::date <= b.reference_date::date
),

daily AS (
    SELECT
        c.user_id,
        c.reference_date,
        c.creation_date,
        c.day,
        coalesce(u.outstanding_balance_eur, 0) AS bal,
        u.max_amount_eur,
        u.od_enabled_flag,

        CASE
            WHEN u.od_enabled_flag = true AND u.max_amount_eur > 0
            THEN coalesce(u.outstanding_balance_eur, 0) / u.max_amount_eur
            ELSE NULL
        END AS util,

        CASE WHEN u.od_enabled_flag = true THEN 1 ELSE 0 END AS is_od_enabled,
        CASE WHEN u.od_enabled_flag = true AND coalesce(u.outstanding_balance_eur, 0) > 0 THEN 1 ELSE 0 END AS is_active_od_draw,
        CASE WHEN u.od_enabled_flag = false AND coalesce(u.outstanding_balance_eur, 0) > 0 THEN 1 ELSE 0 END AS is_technical_od,
        CASE WHEN u.od_enabled_flag = true AND coalesce(u.outstanding_balance_eur, 0) > 0 AND coalesce(u.outstanding_balance_eur, 0) < u.max_amount_eur THEN 1 ELSE 0 END AS partial_repayment

    FROM calendar c
    LEFT JOIN dbt.bp_overdraft_users u
           ON c.user_id = u.user_id
          AND u.timeframe = 'day'
          AND u.end_time::date = c.day
),

daily_feat AS (
    SELECT
        *,
        LAG(util) OVER (PARTITION BY user_id ORDER BY day) AS prev_util,

        CASE WHEN prev_util > 0 AND util = 0 THEN 1 ELSE 0 END AS is_full_repayment,
        CASE WHEN prev_util > util AND util > 0 THEN 1 ELSE 0 END AS is_partial_repayment,
        CASE WHEN util > 0 AND prev_util = 0 THEN 1 ELSE 0 END AS od_entry_event,
        CASE WHEN util = 0 AND prev_util > 0 THEN 1 ELSE 0 END AS od_exit_event,
        CASE WHEN util > 1 THEN 1 ELSE 0 END AS is_overlimit,

        EXTRACT(EPOCH FROM day)::double precision AS day_epoch
    FROM daily
),

streak_prep AS (
    SELECT
        *,
        -- start of active draw streak
        CASE
            WHEN is_active_od_draw = 1 AND COALESCE(LAG(is_active_od_draw) OVER (PARTITION BY user_id ORDER BY day), 0) = 0
            THEN 1
            ELSE 0
        END AS draw_start,
        -- start of overlimit streak
        CASE
            WHEN is_overlimit = 1 AND COALESCE(LAG(is_overlimit) OVER (PARTITION BY user_id ORDER BY day), 0) = 0
            THEN 1
            ELSE 0
        END AS overlimit_start
    FROM daily_feat
),

streaks AS (
    SELECT
        *,
        -- assign streak groups
        SUM(draw_start) OVER (PARTITION BY user_id ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS draw_streak_group,
        SUM(overlimit_start) OVER (PARTITION BY user_id ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS overlimit_streak_group
    FROM streak_prep
),

streak_lengths AS (
    SELECT
        *,

        COUNT(*) OVER (
        PARTITION BY user_id, draw_streak_group
        ORDER BY day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS consecutive_draw_days,

        COUNT(*) OVER (
        PARTITION BY user_id, overlimit_streak_group
        ORDER BY day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS consecutive_overlimit_days

    FROM streaks
),

features AS (
    SELECT
        user_id,
        reference_date,

        -- OD history length
        SUM(is_od_enabled) AS od__history_length_days,
        MAX(is_od_enabled) AS od__is_history,

        -- Average utilization (3m)
        AVG(CASE WHEN day >= reference_date - INTERVAL '90 days' THEN util END)
            AS ob__avg_util_3m,

        -- Avg change (last 30d - 61–90d)
        AVG(CASE WHEN day >= reference_date - INTERVAL '30 days' THEN util END)
        - AVG(CASE WHEN day BETWEEN reference_date - INTERVAL '90 days'
                            AND reference_date - INTERVAL '61 days'
                   THEN util END)
            AS ob__avg_util_change_3m,

        -- Max utilization
        MAX(CASE WHEN day >= reference_date - INTERVAL '90 days' THEN util END)
            AS ob__max_util_3m,

        -- Max change
        MAX(CASE WHEN day >= reference_date - INTERVAL '30 days' THEN util END)
        - MAX(CASE WHEN day BETWEEN reference_date - INTERVAL '90 days'
                            AND reference_date - INTERVAL '61 days'
                   THEN util END)
            AS ob__max_util_change_3m,

        -- Volatility
        STDDEV(CASE WHEN day >= reference_date - INTERVAL '90 days' THEN util END)
            AS ob__std_util_3m,

        STDDEV(CASE WHEN day >= reference_date - INTERVAL '30 days' THEN util END)
        - STDDEV(CASE WHEN day BETWEEN reference_date - INTERVAL '90 days'
                              AND reference_date - INTERVAL '61 days'
                     THEN util END)
            AS ob__std_util_change_3m,

        -- Slope (3m)
        (
            AVG(CASE WHEN day >= reference_date - INTERVAL '90 days' THEN util * day_epoch END)
            - AVG(CASE WHEN day >= reference_date - INTERVAL '90 days' THEN util END)
              * AVG(CASE WHEN day >= reference_date - INTERVAL '90 days' THEN day_epoch END)
        ) /
        NULLIF(
            AVG(CASE WHEN day >= reference_date - INTERVAL '90 days' THEN day_epoch * day_epoch END)
            - POWER(AVG(CASE WHEN day >= reference_date - INTERVAL '90 days' THEN day_epoch END), 2),
            0
        ) AS ob__util_slope_3m,

        -- Range
        MAX(CASE WHEN day >= reference_date - INTERVAL '90 days' THEN util END) -
        MIN(CASE WHEN day >= reference_date - INTERVAL '90 days' THEN util END)
            AS ob__util_range_3m,

        -- OD entries / exits
        SUM(CASE WHEN od_entry_event = 1 AND day >= reference_date - INTERVAL '180 days'
                 THEN 1 ELSE 0 END) AS ob__num_od_entries_6m,

        SUM(CASE WHEN od_exit_event = 1 AND day >= reference_date - INTERVAL '180 days'
                 THEN 1 ELSE 0 END) AS ob__num_od_exits_6m,

        -- Over-limit + repayments
        SUM(CASE WHEN is_overlimit = 1 AND day >= reference_date - INTERVAL '180 days'
                 THEN 1 ELSE 0 END) AS ob__overlimit_days_6m,

        SUM(CASE WHEN is_full_repayment = 1 AND day >= reference_date - INTERVAL '180 days'
                 THEN 1 ELSE 0 END) AS ob__full_repayment_events_6m,

        -- Max consecutive OD draw days
        MAX(
            CASE
                WHEN is_active_od_draw = 1
                THEN consecutive_draw_days
            END
        ) / SUM(is_od_enabled) AS ob__max_consecutive_od_days_since_creation_ratio,

        -- No-drawer / Technical OD features
        SUM(CASE WHEN day >= reference_date - INTERVAL '180 days'
                  AND is_active_od_draw = 1 THEN 1 ELSE 0 END) AS ob__od_draw_days_6m,

        SUM(CASE WHEN day >= reference_date - INTERVAL '180 days'
                  AND is_od_enabled = 1 AND util = 0 THEN 1 ELSE 0 END)
            AS ob__od_enabled_zero_util_days_6m,

        SUM(CASE WHEN day >= reference_date - INTERVAL '180 days'
                  AND is_technical_od = 1 THEN 1 ELSE 0 END)
            AS ob__technical_od_days_6m,

        SUM(CASE WHEN day >= reference_date - INTERVAL '180 days'
                  AND is_technical_od = 1 THEN 1 ELSE 0 END)
            AS ob__technical_od_deep_days_6m,

        -- Avg util in first 2 months after creation
        AVG(CASE WHEN day BETWEEN creation_date AND creation_date + INTERVAL '60 days'
                 THEN util END) AS ob__avg_util_first_2m_since_creation,

        -- Change in avg util since origination
        AVG(CASE WHEN day BETWEEN reference_date - INTERVAL '60 days' AND reference_date
                 THEN util END)
        - AVG(CASE WHEN day BETWEEN creation_date AND creation_date + INTERVAL '60 days'
                   THEN util END) AS ob__avg_util_change_last2m_to_first2m,

        -- Avg util 1 year ago
        AVG(CASE WHEN day BETWEEN reference_date - INTERVAL '365 days'
                            AND reference_date - INTERVAL '335 days'
                 THEN util END) AS ob__avg_util_first2m_1y,

        -- Change in avg util over 1 year
        AVG(CASE WHEN day BETWEEN reference_date - INTERVAL '60 days' AND reference_date
                 THEN util END)
        - AVG(CASE WHEN day BETWEEN reference_date - INTERVAL '365 days'
                            AND reference_date - INTERVAL '335 days'
                   THEN util END) AS ob__avg_util_change_last2m_to_first2m_1y,

        -- Max consecutive overlimit days
        MAX(
            CASE
                WHEN is_overlimit = 1
                THEN consecutive_overlimit_days
            END
        ) / SUM(is_od_enabled) AS ob__max_consecutive_overlimit_days_since_creation_ratio,

        -- Partial repayment patterns
        SUM(CASE WHEN is_partial_repayment = 1 AND day >= reference_date - INTERVAL '180 days'
                 THEN 1 ELSE 0 END)
            AS ob__partial_repayment_events_6m,

        -- Change in repayment behavior
        SUM(CASE WHEN day >= reference_date - INTERVAL '90 days'
                  AND day < reference_date THEN is_full_repayment END)
        - SUM(CASE WHEN day >= reference_date - INTERVAL '180 days'
                    AND day < reference_date - INTERVAL '90 days'
                   THEN is_full_repayment END)
            AS ob__full_repayment_change_3m_vs_prev_3m,

        -- Liquidity ratios
        AVG(CASE WHEN day BETWEEN reference_date - INTERVAL '60 days' AND reference_date
                 THEN util END)
        / NULLIF(
            AVG(CASE WHEN day BETWEEN creation_date AND creation_date + INTERVAL '60 days'
                     THEN util END),
            0
        ) AS ob__util_last2m_to_first2m_ratio,

        AVG(CASE WHEN day BETWEEN reference_date - INTERVAL '60 days' AND reference_date
                 THEN util END)
        / NULLIF(
            AVG(CASE WHEN day BETWEEN reference_date - INTERVAL '365 days'
                                AND reference_date - INTERVAL '335 days'
                     THEN util END),
            0
        ) AS ob__util_last2m_to_first2m_1y_ratio,

        MAX(CASE WHEN day >= reference_date - INTERVAL '90 days' THEN util END)
        / NULLIF(
            AVG(CASE WHEN day >= reference_date - INTERVAL '90 days' THEN util END),
            0
        ) AS ob__max_to_avg_util_3m_ratio

    FROM streak_lengths
    GROUP BY 1, 2
)

SELECT *
FROM features