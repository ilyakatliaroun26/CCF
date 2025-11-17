select 
user_id
, reference_date
, "LIMIT" as ob__limit_ref
, BALANCE as ob__balance_ref
, od_utilization_current as ob__avg_util_ref
, case when is_drawn = true then 1 else 0 end as t__is_drawn
, CCF as t__ccf
from credit_risk_playground.bp_od_ccf_training_snapshot