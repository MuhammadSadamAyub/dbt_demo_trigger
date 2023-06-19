
with source_fact_memberaccesshistory as (

    select mx.OutreachId
	, mx.FactMemberAccessHistoryKey
	, mx.CallDateKey
	, mx.UserKey
	, mx.AccessStart
	, mx.MemberCallHistoryId
	, mx.MemberPlanId
	, mx.AccessMethodKey
	, mx.Resulting_CallCodeKey
	, mx.AccessEnd
	, mx.CallTimeKey
	, mx.SchedulerUserKey
    from {{ ref('stg_censeonanalysis__fact_memberhistory') }} mx 
    where mx.CallDateKey >= {{ var("backdatekey") }}  -- backdatekey


),
 final as (
select *
from source_fact_memberaccesshistory where OutreachId is not null or OutreachId <> '00000000-0000-0000-0000-000000000000'
)

select * from final 
 