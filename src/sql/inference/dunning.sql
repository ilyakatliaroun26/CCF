with base as (
    select distinct 
    user_id, 
    end_time as reference_date
    from dbt.bp_overdraft_users
    where end_time::date = last_day(date_add('month', -1, last_day(getdate())))::date -- reference date to change
    and timeframe = 'day'
    and od_enabled_flag = true
),

dunning_log AS (
    SELECT 
        l.user_id,
        l.created_at::date AS action_date,
        l.dunning_process_id,
        la.name AS raw_action
    FROM lanthanum_action_logs l
    INNER JOIN lanthanum_actions la 
        ON la.id = l.actions_id

    UNION ALL

    SELECT DISTINCT
        c.user_id,
        dp.created::date AS action_date,
        dp.dunning_process_id,
        REPLACE(ast.name, ' ', '_') AS raw_action
    FROM aspirin_action_log dp
    INNER JOIN carbonium_user_account c 
        ON c.account_id = dp.account_id
    INNER JOIN aspirin_dunning_process_task_definition t 
        ON t.id = dp.task_id
    INNER JOIN aspirin_dunning_process_step_definition ast
        ON t.dunning_process_step = ast.id
),

dunning_actions AS (
    SELECT
        user_id,
        action_date,
        dunning_process_id,
        CASE
            WHEN raw_action ILIKE '%FIRST_FRIENDLY_REMINDER%' OR raw_action ILIKE '%FIRST_FRIENDLY_NOTIFICATION%' THEN 1
            WHEN raw_action ILIKE '%SECOND_FRIENDLY_REMINDER%' OR raw_action ILIKE '%SECOND_FRIENDLY_NOTIFICATION%' THEN 7
            WHEN raw_action ILIKE '%THIRD_FRIENDLY_REMINDER%' OR raw_action ILIKE '%THIRD_FRIENDLY_NOTIFICATION%' THEN 10
            WHEN raw_action ILIKE '%FOURTH_FRIENDLY_FIRST_REMINDER%' OR raw_action ILIKE '%FOURTH_FRIENDLY_FIRST_NOTIFICATION%' THEN 14
            WHEN raw_action ILIKE '%FOURTH_FRIENDLY_SECOND_REMINDER%' OR raw_action ILIKE '%FOURTH_FRIENDLY_SECOND_NOTIFICATION%' THEN 30
            WHEN raw_action ILIKE '%FIRST_SERIOUS_REMINDER%' OR raw_action ILIKE '%FIRST_SERIOUS_NOTIFICATION%' THEN 45
            WHEN raw_action ILIKE '%SECOND_SERIOUS_REMINDER%' OR raw_action ILIKE '%SECOND_SERIOUS_NOTIFICATION%' THEN 60
            WHEN raw_action ILIKE '%THIRD_SERIOUS_REMINDER%' OR raw_action ILIKE '%THIRD_SERIOUS_NOTIFICATION%' THEN 75
            WHEN raw_action ILIKE '%FOURTH_SERIOUS_REMINDER%' OR raw_action ILIKE '%FOURTH_SERIOUS_NOTIFICATION%' THEN 80
            WHEN raw_action ILIKE '%FIRST_OFFICIAL_REMINDER%' OR raw_action ILIKE '%FIRST_OFFICIAL_NOTIFICATION%' THEN 90
            WHEN raw_action ILIKE '%SECOND_OFFICIAL_REMINDER%' OR raw_action ILIKE '%SECOND_OFFICIAL_NOTIFICATION%' THEN 120
            ELSE 0
        END AS action
    FROM dunning_log
),

dunning_window AS (
    SELECT 
        b.user_id,
        b.reference_date,
        d.action_date,
        d.action
    FROM base b
    JOIN dunning_actions d
        ON b.user_id = d.user_id
       AND d.action_date BETWEEN (b.reference_date - INTERVAL '365 days') AND b.reference_date
),

recent_action AS (
    SELECT user_id,
           reference_date,
           action AS dn__recent_action
    FROM (
        SELECT 
            b.user_id,
            b.reference_date,
            d.action,
            ROW_NUMBER() OVER (
                PARTITION BY b.user_id, b.reference_date
                ORDER BY d.action_date DESC
            ) AS rn
        FROM base b
        JOIN dunning_actions d
            ON b.user_id = d.user_id
           AND d.action_date BETWEEN (b.reference_date - INTERVAL '365 days') AND b.reference_date
        WHERE d.action <> 0   -- exclude 'other'
    ) t
    WHERE rn = 1
),

-- Max action CTE
max_action AS (
    SELECT
        user_id,
        reference_date,
        MAX(action) AS dn__max_action_level
    FROM dunning_window
    GROUP BY user_id, reference_date
)

SELECT
    r.user_id,
    r.reference_date,
    r.dn__recent_action,
    m.dn__max_action_level
FROM recent_action r
JOIN max_action m
    ON r.user_id = m.user_id AND r.reference_date = m.reference_date