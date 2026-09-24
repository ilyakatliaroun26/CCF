with base as (
    select distinct 
    user_id, 
    end_time as reference_date
    from dbt.bp_overdraft_users
    where end_time::date = last_day(date_add('month', -1, last_day(getdate())))::date -- reference date to change
    and timeframe = 'day'
    and od_enabled_flag = true
),

madrid_filtered as (

    select
        initiator_user_id as user_id,
        reference_date,
        transaction_id,
        display_timestamp::date as transaction_date,
        transaction_type,
        detailed_category,
        transaction_amount
    from dbt.madrid_transactions md
    inner join base
        on base.user_id = md.initiator_user_id
        and md.display_timestamp::date between dateadd(MONTHS, -3, base.reference_date - INTERVAL '1 DAY') and base.reference_date
    where (
                (
                    transaction_type in ('TRANSACTION_TYPE_CT', 'TRANSACTION_TYPE_DT', 'TRANSACTION_TYPE_DD', 'TRANSACTION_TYPE_FT')
                    and detailed_category != 'PayBackFromSpace'
                ) or
                (
                    transaction_type in ('TRANSACTION_TYPE_PF', 'TRANSACTION_TYPE_PT')
                )
        )
        and (
            detailed_category ilike 'income%%' or
            detailed_category ilike 'expenses%%'
        )

),

madrid as (

    select
        user_id,
        reference_date,
        case
            when transaction_date::date between dateadd(MONTHS, -3, reference_date - INTERVAL '1 DAY') and dateadd(MONTHS, -2, reference_date - INTERVAL '2 DAYS')
                THEN 'M1'
            when transaction_date::date between dateadd(MONTHS, -2, reference_date - INTERVAL '1 DAY') and dateadd(MONTHS, -1, reference_date - INTERVAL '2 DAYS')
                THEN 'M2'
            when transaction_date::date between dateadd(MONTHS, -1, reference_date - INTERVAL '1 DAY') and reference_date
                then 'M3'
        end as monthly_tx_cohort,
        sum(
            case when detailed_category ilike 'income%%'
            then abs(transaction_amount) else 0.0 end
        ) as monthly_income,
        sum(
            case when detailed_category in (
                'Income:Employment:Salary',
                'Income:SelfEmployment:FreelanceIncome'
            ) then abs(transaction_amount) else 0.0 end
        ) as monthly_salary,
        sum(
            case when detailed_category in (
                'Expenses:Basics:Groceries',
                'Expenses:Basics:SubscriptionsAndRecurringPayments',
                'Expenses:Health:HealthcareExpenses',
                'Expenses:Housing:Rent',
                'Expenses:Housing:Utilities'
            ) then abs(transaction_amount) else 0.0 end
        ) as monthly_basic_expenses,
        sum(
            case when detailed_category ilike 'expenses%%'
                then abs(transaction_amount) else 0.0
            end
        ) as monthly_expenses
    from madrid_filtered
    group by 1, 2, 3

),

agg_table as (

    select
        user_id,
        reference_date,
        avg(monthly_income) as avg_monthly_income,
        avg(monthly_salary) as madrid__avg_monthly_salary_3m,
        avg(monthly_expenses) as madrid__avg_monthly_expenses_3m,
        avg(monthly_basic_expenses) as avg_monthly_basic_expenses
    from madrid
    group by 1, 2

)

select distinct
    base.user_id
    , base.reference_date
    , md.madrid__avg_monthly_salary_3m
    , md.madrid__avg_monthly_expenses_3m
    , case when
        (avg_monthly_income is null) and (avg_monthly_basic_expenses is null)
            then null
        else coalesce(avg_monthly_income, 0) - coalesce(avg_monthly_basic_expenses, 0)
    end as madrid__avg_monthly_repayment_capacity_3m
from base
left join agg_table as md
    on base.user_id = md.user_id
    and base.reference_date = md.reference_date
