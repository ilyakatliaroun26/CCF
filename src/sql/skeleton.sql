drop table credit_risk_playground.bp_ccf_training_snapshot
;
create table if not exists credit_risk_playground.bp_ccf_training_snapshot
(
	user_id VARCHAR(365)   ENCODE lzo
	, dunning_second_reminder_date VARCHAR(365)   ENCODE lzo
    , dunning_downgrade_date VARCHAR(365)   ENCODE lzo
    , dunning_closure_date VARCHAR(365)   ENCODE lzo
    , write_off_date VARCHAR(365)   ENCODE lzo
    , dpd_90_date VARCHAR(365)   ENCODE lzo
    , d26_insolvency_date VARCHAR(365)   ENCODE lzo
    , infocard_date VARCHAR(365)   ENCODE lzo
    , schufa_insolvency_date VARCHAR(365)   ENCODE lzo
    , crif_insolvency_date VARCHAR(365)   ENCODE lzo
    , internal_default_date VARCHAR(365)   ENCODE lzo
    , default_date VARCHAR(365)   ENCODE lzo
    , default_reason VARCHAR(365)   ENCODE lzo
)

;

insert into credit_risk_playground.bp_ccf_training_snapshot
(
    user_id,
    dunning_second_reminder_date,
    dunning_downgrade_date,
    dunning_closure_date,
    write_off_date,
    dpd_90_date,
    d26_insolvency_date,
    infocard_date,
    schufa_insolvency_date,
    crif_insolvency_date,
    internal_default_date,
    default_date,
    default_reason
)
/*  ---------------------------------------------------
    Default data ETL description
    Owner: Credit Risk
    Date: 2022-12-01
    Version: v11

    From RFC on Miracle (first version):

    The current process in PD model development
    is to extract default data based on using all
    historically available data in DWH since 2017.
    At the moment we query all of the sources into one
    master table with default dates for each user_id.

    Therefore, from the Credit Risk side, the proposal is
    to keep it simple and replicate the existing logic of
    sourcing all historically available default data for
    Lisbon into the new structure.

    The following default reasons are processed:

    Internal:
    --
    1) Overdraft loan cancellations (dunning)
    2) N26 bankruptcy flags
    3) Write-offs
        Note: not a default reason, but we add it as a
        bad flag in rder not to miss any default events
    4) TBIL loan cancellations (infocards)

    External:
    --
    6) Schufa UTP triggers (Merkmale)
    7) CRIF UTP triggers (P score)
    --------------------------------------------------- */

/*  ---------------------------------------------------
    Base
    --------------------------------------------------- */

with rep_plan as (
select distinct user_id 
from plutonium_repayment_plan
)

, pu_first_row as (
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
        case when osa.status = 'ENABLED' then 1 else 0 end as enabled,
        case when r.user_id is not null then 'RP' else 'OD' end as product
    from pu_overdraft_history as osa
    inner join pu_first_row as pfr using (user_id)
    left join dbt.mmbr_user_match cl on cl.user_id = osa.user_id
    left join mmbr_savings_account s on s.encoded_key = cl.encoded_key and s.account_type = 'CURRENT_ACCOUNT'
    left join rep_plan r on r.user_id = osa.user_id
    where 1=1
    and rev_timestamp >= pfr.min_rev_timestamp
	-- todo: filter more detailed for migration timestamp

    union all

    -- Include rows from DDB before the first Plutonium populated date
    select
        u.id as user_id,
        s.encoded_key as instrument_id,
        osa.rev_timestamp,
        osa.enabled,
        case when r.user_id is not null then 'RP' else 'OD' end as product
    from ddb_overdraft_settings_aud as osa
    inner join etl_reporting.cmd_users as u using (user_created)
    left join pu_first_row as pfr
        on u.id = pfr.user_id
    left join dbt.mmbr_user_match cl on cl.user_id = u.id
    left join mmbr_savings_account s on s.encoded_key = cl.encoded_key and s.account_type = 'CURRENT_ACCOUNT'
    left join rep_plan r on r.user_id = u.id
    where
        osa.rev_timestamp < pfr.min_rev_timestamp -- Include only historical records before Plutonium migration
)

, overdrafts AS (
SELECT
user_id
, instrument_id
, min(rev_timestamp::TIMESTAMP) AS creation_date
FROM
od_users
where product <> 'RP'
and enabled = 1
GROUP BY 1, 2
)

/*  ---------------------------------------------------
    Overdraft loan cancellations (dunning)
    --------------------------------------------------- */

-- Pre-Lanthanum data (before 2019)
, old_dunning_status as (
    select
        a.id as user_id,
        min(case
                when status in ('BO_2nd_Official_Notice','BO_2nd_Official_Notice_OD')
                then rev_tstmp::date end) dunning_second_reminder_date,
        min(case
                when status in ('BO_Revocation_Premium')
                then rev_tstmp::date end) dunning_downgrade_date,
        min(case
                when status in ('Account_closed', 'BO_Account_Closure')
                then rev_tstmp::date end) dunning_closure_date
    from
        public.dwh_portfolio_status_aud a
    inner join overdrafts o on o.user_id = a.id
        where status in ('BO_2nd_Official_Notice','BO_2nd_Official_Notice_OD',
                         'BO_Revocation_Premium','Account_closed','BO_Account_Closure')
            and rev_tstmp::date <= ('2024-09-30'::date + INTERVAL '1 YEAR 3 DAYS')
            and o.creation_date::date <= a.rev_tstmp::date
    group by
        a.id
),

-- Lanthanum data (after 2019)
lanthanum_action_lg as (
    select 
        l.user_id,
        l.created_at,
        l.dunning_process_id,
        la.name as action
    from public.lanthanum_action_logs l 
    inner join public.lanthanum_actions la 
        on l.actions_id = la.id

    union all 

    -- Aspirin from September 2024
    select distinct
        c.user_id,
         dp.created::date as created_at,
         dp.dunning_process_id,
         replace(ast.name,' ', '_') as action
    from public.aspirin_action_log as dp
    inner join public.carbonium_user_account c 
        on c.account_id = dp.account_id
    inner join public.aspirin_dunning_process_task_definition t 
        on t.id = dp.task_id
    inner join public.aspirin_dunning_process_step_definition ast 
        on t.dunning_process_step = ast.id
),

-- Lanthanum data (after 2019)
new_dunning_status as (
    select
        a.user_id,
        min(case
                when action in ('SECOND_OFFICIAL_NOTIFICATION', 'SEND_SECOND_OFFICIAL_REMINDER_EMAIL', 'SECOND_OFFICIAL_REMINDER')
                then created_at::date end) dunning_second_reminder_date,
        min(case
                when action in ('SEND_USER_DOWNGRADE_EMAIL')
                then created_at::date end) dunning_downgrade_date,
        min(case
                when action in ('SEND_CLOSURE_EMAIL', 'ACCOUNT_CLOSED_EMAIL')
                then created_at::date end) dunning_closure_date
    from lanthanum_action_lg a
    inner join overdrafts o on o.user_id = a.user_id
    where action in ('SECOND_OFFICIAL_NOTIFICATION','SEND_SECOND_OFFICIAL_REMINDER_EMAIL',
                         'SEND_USER_DOWNGRADE_EMAIL','SEND_CLOSURE_EMAIL', 
                         'ACCOUNT_CLOSED_EMAIL', 'SECOND_OFFICIAL_REMINDER')
        and created_at::date <= ('2024-09-30'::date + INTERVAL '1 YEAR 3 DAYS')
        and o.creation_date::date <= a.created_at::date
    group by
        a.user_id
),

-- Historical dunning data
dunning_status as (
    select *
    from old_dunning_status

    union

    select *
    from new_dunning_status
),

-- Dunning default reasons
dunning as (
    select
        d.user_id::text as user_id,
        min(d.dunning_second_reminder_date) as dunning_second_reminder_date,
        min(d.dunning_downgrade_date) as dunning_downgrade_date,
        min(d.dunning_closure_date) as dunning_closure_date
    from dunning_status d
    group by 1
),

/*  ---------------------------------------------------
    N26 bankruptcy flag
    (internal, coming from Neodymium as insolvency)

    Neodymium will be the root source for N26 internal
    bankruptcy events that will allow Miracle to gather
    when a customer was considered insolvent in a timely
    manner (i.e. customer bankruptcy flag STARTED -> CLOSED,
    CLOSED -> STARTED, STARTED -> ON_HOLD, ...)
    --------------------------------------------------- */

Insolve26 as (
    select
        np.customer_id as user_id,
        min(ni.origin_date::date) d26_insolvency_date
    from public.neodymium_insolvency ni
    left join public.neodymium_participant np on ni.id = np.garnishment_id
    inner join overdrafts o on o.user_id = np.customer_id
    where np.type = 'RESPONDENT'
        and ni.origin_date::date <= ('2024-09-30'::date + INTERVAL '1 YEAR 3 DAYS')
        and np.created::date <= ('2024-09-30'::date + INTERVAL '1 YEAR 3 DAYS')
        and o.creation_date::date <= ni.origin_date::date
        and o.creation_date::date <= np.created::date
    group by np.customer_id
),

/*  ---------------------------------------------------
    Write-offs
    Change since Oct 22: max function was changed
    to min to cover the first write-off event
    --------------------------------------------------- */

base as (
    select
        a.user_id,
        case
            when sum(case when closure_type = 'AML' then 1 else 0 end ) >=1 then 'AML'
            when sum(case when closure_type = 'DUNNING' then 1 else 0 end) >= 1 then 'DUNNING'
            else 'OTHER'
        end as closure_type,
        max(write_off_dt) as write_off_dt,
        sum(eur_value)*-1 as "value"
    from dbt.ucm_stg_od_write_off_txns a
    inner join overdrafts o on o.user_id = a.user_id
    where write_off_dt::date <= ('2024-09-30'::date + INTERVAL '1 YEAR 3 DAYS')
    and o.creation_date::date <= a.write_off_dt::date
    group by 1
),

od_write_off_agg as (
    select
        t.user_id,
        t.write_off_dt
    from base as t
    where "value" < 0
),

daily_arrears as (
    select 
        a.user_id, 
        b.end_time,
        datediff('day', start_in_arrears::date, b.end_time::date) + 1 as dpd
    from dbt.bp_arrears_reg_aud a
    inner join dwh_cohort_dates b on b.end_time between a.start_in_arrears and a.end_in_arrears
),

defaulted_arrears as (
    select 
        da.user_id, 
        min(da.end_time) as dpd_90_date
    from daily_arrears da
    inner join overdrafts o on o.user_id = da.user_id
    where end_time::date <= ('2024-09-30'::date + INTERVAL '1 YEAR 3 DAYS')
    and o.creation_date::date <= da.end_time::date
    and dpd = 90
    group by da.user_id
),

credit_write_offs as (
    select
        mum.user_id,
        mla.encoded_key,
        substring(
            mla.encoded_key, 0, 9
        ) || '-' || substring(mla.encoded_key, 9, 4) || '-'
        || substring(
            mla.encoded_key, 13, 4
        ) || '-' || substring(
            mla.encoded_key, 17, 4
        ) || '-' || substring(mla.encoded_key, 21, 12)
        as encoded_key_old_format,
        case
            when lpm.product = 'consumer_credit' then 'Credit'
            when lpm.product = 'installment_loans' then 'TBIL'
            when lpm.product = 'repayment_plans' then 'RP_PHASE2'
            else lpm.product end as credit_product,
        c.rp_phase_1_user,
        c.suspicious_closure,
        c.missing_closure_on,
        c.closure_type as main_account_closure_type,
        c.write_off_dt as main_account_write_off_dt,
        max(lt.entry_date) as write_off_dt
    from public.mmbr_loan_transaction as lt
    inner join public.mmbr_loan_account as mla
        on lt.parent_account_key = mla.encoded_key
            and lt.type = 'WRITE_OFF'
            and mla.account_state = 'CLOSED_WRITTEN_OFF'
            and lt.reversal_transaction_key is null -- exclude write-offs that were reverted
    inner join dbt.mmbr_loan_product_mapping as lpm
        on mla.loan_name = lpm.loan_name
            and lpm.product in ('installment_loans', 'consumer_credit', 'repayment_plans')
    inner join dbt.mmbr_user_match as mum
        on mla.account_holder_key = mum.mmbr_client_key
    left join dbt.stg_write_off_closures as c using (user_id)
    inner join overdrafts o on o.user_id = mum.user_id
    where lt.entry_date <= ('2024-09-30'::date + INTERVAL '1 YEAR 3 DAYS')
    and o.creation_date::date <= lt.entry_date::date
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
),

all_write_offs as (
    select
        t.user_id,
        t.write_off_dt::date as write_off_dt
    from od_write_off_agg as t
    inner join customer_user_closure as c
        using (user_id)

    union all

    select
        user_id,
        write_off_dt::date as write_off_dt
    from credit_write_offs
),

write_offs as (
    select
        user_id,
        min(write_off_dt) as write_off_date
    from all_write_offs
    group by 1
),

/*  ---------------------------------------------------
    TBIL loan cancellations (infocards)
    Change since Oct 22: introduced to capture loan
    cancellations for TBIL loans
    Source: TBIL Collection and Arrears Dashboard
    --------------------------------------------------- */

TBIL_infocards as (
    select
        i.user_id,
        t.name,
        i.created::date as infocard_date
    from
        public.moscovium_infocard_template t
    left join dbt.infocards i on i.template_id = t.id
    inner join overdrafts o on o.user_id = i.user_id
    -- tbil dunning-related infocards
    where t.name in ('TRANSACTION_BASED_INSTALMENT_LOAN_COLLECTION_PROCESS_ACTION_FOUR')
    and i.created::date >= '2020-11-29' --first infocard created (source TBIL arrears dashboard)
    and i.created::date <= ('2024-09-30'::date + INTERVAL '1 YEAR 3 DAYS')
    and o.creation_date::date <= i.created::date
),

tbil_cancellations as (
    select
        z.user_id,
        min(infocard_date) as infocard_date

    from
        TBIL_infocards
    join
        dbt.zrh_users z using (user_id)
    group by
        z.user_id
),

/*  ---------------------------------------------------
    Schufa UTP triggers (only for DEU users)

    Credit Risk Policy
    --
    Hard negative features:
    EV : Affidavit/execution based on the list of assets not suitable or not provided (within one month)
    HB : Arrest warrant/no submission of list of assets
    IA : Application for the initiation of (streamlined) insolvency proceedings
    IE : (streamlined) Insolvency proceedings initiated
    IS : Insolvency proceedings set aside
    RA :  Discharge of residual debt announced
    RV : Discharge of residual debt denied
    S1 :  No submission of list of assets
    S2 : Execution based on the contents of the list of assets not suitable to satisfy creditors
    S3 : Debtor has not demonstrated the creditor has been satisfied in full within one month of submission of the list of assets
    --------------------------------------------------- */

ccra as (
    select
        a.user_id,
        requested_on::date as rev_timestamp
    from private.californium_credit_score_audit_log a
    inner join overdrafts o on o.user_id = a.user_id
    where rating in ('N', 'O', 'P')
        and provider like 'SCHUFA%%'
        and requested_on::date <= ('2024-09-30'::date + INTERVAL '1 YEAR 3 DAYS')
        and o.creation_date::date <= a.requested_on::date
),

csr as (
    select
        a.user_id,
        requested_on::date as rev_timestamp
    from private.californium_credit_score_record_request_merkmal_audit_log a
    inner join overdrafts o on o.user_id = a.user_id
    where (
        merkmalcode in ('EV', 'HB', 'IA', 'IE', 'IS', 'RA', 'RV', 'S1', 'S2', 'S3')
            and (
                json_serialize(merkmal) not like '%%nachmeldegrund%%'
                or (
                    json_serialize(merkmal) not like '%%lösch%%' and 
                    json_serialize(merkmal) not like '%%erledigungsvermerk%%'
                )
            )
        )
        and requested_on::date <= ('2024-09-30'::date + INTERVAL '1 YEAR 3 DAYS')
        and o.creation_date::date <= a.requested_on::date
),

schufa as (
    select
        ccra.user_id,
        min(ccra.rev_timestamp) as rev_timestamp
    from ccra
    inner join csr
        on ccra.user_id = csr.user_id
        and csr.rev_timestamp = ccra.rev_timestamp
    group by
        ccra.user_id
),


/*  ---------------------------------------------------
    CRIF UTP triggers (only for AUT users)

    Credit Risk Policy
    --
    Austria: Rating is P
    --------------------------------------------------- */

crif as (
    select
        a.user_id,
        min(requested_on::date) as rev_timestamp
    from private.californium_credit_score_audit_log a
    inner join overdrafts o on o.user_id = a.user_id
    where rating in ('P')
            and provider in ('CRIF')
            and requested_on::date <= ('2024-09-30'::date + INTERVAL '1 YEAR 3 DAYS')
            and o.creation_date::date <= a.requested_on::date
    group by
    a.user_id
)

select
    a.user_id,
    -- default reasons
    d.dunning_second_reminder_date::date as dunning_second_reminder_date,
    d.dunning_downgrade_date::date as dunning_downgrade_date,
    d.dunning_closure_date::date as dunning_closure_date,
    w.write_off_date::date as write_off_date,
    da.dpd_90_date::date as dpd_90_date,
    ins.d26_insolvency_date::date as d26_insolvency_date,
    m.infocard_date::date as infocard_date,
    s.rev_timestamp::date as schufa_insolvency_date,
    c.rev_timestamp::date as crif_insolvency_date,

    /* due to the fact that users could in
       the past sign up with a negative credit
       bureau entry, we first process
       internal default reasons and if there
       is none, we use an external trigger */

    least(dunning_second_reminder_date,
        dunning_closure_date,
        dunning_downgrade_date,
        write_off_date,
        dpd_90_date,
        d26_insolvency_date,
        infocard_date) as internal_default_date,

    /* processing first internal and then external defaults */
    case when internal_default_date is null
    then least(schufa_insolvency_date, crif_insolvency_date)
    else internal_default_date end as default_date,

    /* assigning a default reason */
    case
        when default_date = dunning_second_reminder_date then 'dunning_second_reminder_date'
        when default_date = dunning_downgrade_date then 'dunning_downgrade_date'
        when default_date = dunning_closure_date then 'dunning_closure_date'
        when default_date = write_off_date then 'write_off_date'
        when default_date = dpd_90_date then 'dpd_90_date'
        when default_date = d26_insolvency_date then 'n26_insolvency_date'
        when default_date = infocard_date then 'tbil_infocard_date'
        when default_date = schufa_insolvency_date then 'schufa_insolvency_date'
        when default_date = crif_insolvency_date then 'crif_insolvency_date'
        else null
    end as default_reason

from overdrafts a

left join dunning d using(user_id)
left join write_offs w using(user_id)
left join Insolve26 ins using(user_id)
left join schufa s using(user_id)
left join crif c using(user_id)
left join tbil_cancellations m using(user_id)
left join defaulted_arrears da using(user_id)

where least(
    dunning_second_reminder_date,
    dunning_closure_date,
    dunning_downgrade_date,
    write_off_date,
    dpd_90_date,
    d26_insolvency_date,
    infocard_date,
    schufa_insolvency_date,
    crif_insolvency_date
) is not null