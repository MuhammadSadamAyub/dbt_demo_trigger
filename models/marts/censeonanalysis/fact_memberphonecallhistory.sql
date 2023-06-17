with fact_memberphonecallhistory_src as (

select 
{{ dbt_utils.star(from=ref('stg_censeonanalysis__src_fact_memberphonecallhistory'), except=['CallCodeKey','CallDateKey'
   ,'CallTimeKey','PhoneCallStatusId','MemberPhoneId','SchedulerUserKey','MemberPlanId','IsOutboundCall','ETLUpdateTS','ETLHashbytes',
   'CreatedDate','OutreachId','UserKey','AccessMethodKey','FactMemberAccessHistoryKey']) }},

 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.CallCodeKey ELSE  T.CallCodeKey END ) AS CallCodeKey,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.CallTimeKey ELSE  T.CallTimeKey END ) AS CallTimeKey,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.PhoneCallStatusId ELSE  T.PhoneCallStatusId END ) AS PhoneCallStatusId,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.MemberPhoneId ELSE  T.MemberPhoneId END ) AS MemberPhoneId,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.SchedulerUserKey ELSE  T.SchedulerUserKey END ) AS SchedulerUserKey,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.MemberPlanId ELSE  T.MemberPlanId END ) AS MemberPlanId,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.IsOutboundCall ELSE  T.IsOutboundCall END ) AS IsOutboundCall,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  CURRENT_DATE() ELSE  T.ETLUpdateTS END ) AS ETLUpdateTS,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.ETLHashbytes ELSE  T.ETLHashbytes END ) AS ETLHashbytes,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.CreatedDate ELSE  T.CreatedDate END ) AS CreatedDate,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.OutreachId ELSE  T.OutreachId END ) AS OutreachId,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.UserKey ELSE  T.UserKey END ) AS UserKey,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.AccessMethodKey ELSE  T.AccessMethodKey END ) AS AccessMethodKey,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.FactMemberAccessHistoryKey  ELSE  T.FactMemberAccessHistoryKey END ) AS FactMemberAccessHistoryKey

from  
{{ ref('stg_censeonanalysis__src_fact_memberphonecallhistory') }} T
Left Join 
{{ ref('int_censeonanalysis__phonecallhistory__src') }} S
on  T.PhoneCallHistoryId = S.PhoneCallHistoryId
AND ( T.ETLHashbytes != S.ETLHashbytes ) 
AND S.PhoneCallHistoryId > 0
),

factmemberphonecallhistory_keyupdates as 
(

SELECT t.FactMemberPhoneCallHistoryKey 
FROM {{ ref('int_censeonanalysis__phonecallhistory__src') }} S
INNER JOIN fact_memberphonecallhistory_src T  
ON T.PhoneCallHistoryId = S.PhoneCallHistoryId
WHERE t.FactMemberAccessHistoryKey != s.FactMemberAccessHistoryKey
AND S.PhoneCallHistoryId > 0
AND T.PhoneCallHistoryId > 0
),

update_fact_memberphonecalhistory as 
(

select 
{{ dbt_utils.star(from=ref('stg_censeonanalysis__src_fact_memberphonecallhistory'), except=['CallCodeKey','CallDateKey'
   ,'CallTimeKey','PhoneCallStatusId','MemberPhoneId','SchedulerUserKey','MemberPlanId','IsOutboundCall','ETLUpdateTS','ETLHashbytes',
   'CreatedDate','OutreachId','UserKey','AccessMethodKey','FactMemberAccessHistoryKey']) }},

 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.CallCodeKey ELSE  T.CallCodeKey END ) AS CallCodeKey,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.CallTimeKey ELSE  T.CallTimeKey END ) AS CallTimeKey,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.PhoneCallStatusId ELSE  T.PhoneCallStatusId END ) AS PhoneCallStatusId,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.MemberPhoneId ELSE  T.MemberPhoneId END ) AS MemberPhoneId,
  
  ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.SchedulerUserKey ELSE  T.SchedulerUserKey END ) AS SchedulerUserKey,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.MemberPlanId ELSE  T.MemberPlanId END ) AS MemberPlanId,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.IsOutboundCall ELSE  T.IsOutboundCall END ) AS IsOutboundCall,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  CURRENT_DATE() ELSE  T.ETLUpdateTS END ) AS ETLUpdateTS,

 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.ETLHashbytes ELSE  T.ETLHashbytes END ) AS ETLHashbytes,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.CreatedDate ELSE  T.CreatedDate END ) AS CreatedDate,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.OutreachId ELSE  T.OutreachId END ) AS OutreachId,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.UserKey ELSE  T.UserKey END ) AS UserKey,

 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.AccessMethodKey ELSE  T.AccessMethodKey END ) AS AccessMethodKey,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.FactMemberAccessHistoryKey  ELSE  T.FactMemberAccessHistoryKey END ) AS FactMemberAccessHistoryKey

from  
fact_memberphonecallhistory_src T
Left Join 
{{ ref('int_censeonanalysis__phonecallhistory__src') }} S
on  T.PhoneCallHistoryId = S.PhoneCallHistoryId
AND t.FactMemberAccessHistoryKey != s.FactMemberAccessHistoryKey
AND S.PhoneCallHistoryId > 0
AND T.PhoneCallHistoryId > 0
Left join factmemberphonecallhistory_keyupdates K
on  T.FactMemberPhoneCallHistoryKey = K.FactMemberPhoneCallHistoryKey
),
update_fact_memberphonecalhistory_1 as 
(

select 
{{ dbt_utils.star(from=ref('stg_censeonanalysis__src_fact_memberphonecallhistory'), except=['CallCodeKey','CallDateKey'
   ,'CallTimeKey','PhoneCallStatusId','MemberPhoneId','SchedulerUserKey','MemberPlanId','IsOutboundCall','ETLUpdateTS','ETLHashbytes',
   'CreatedDate','OutreachId','UserKey','AccessMethodKey','FactMemberAccessHistoryKey']) }},

 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.CallCodeKey ELSE  T.CallCodeKey END ) AS CallCodeKey,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.CallTimeKey ELSE  T.CallTimeKey END ) AS CallTimeKey,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.PhoneCallStatusId ELSE  T.PhoneCallStatusId END ) AS PhoneCallStatusId,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.MemberPhoneId ELSE  T.MemberPhoneId END ) AS MemberPhoneId,
  
  ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.SchedulerUserKey ELSE  T.SchedulerUserKey END ) AS SchedulerUserKey,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.MemberPlanId ELSE  T.MemberPlanId END ) AS MemberPlanId,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.IsOutboundCall ELSE  T.IsOutboundCall END ) AS IsOutboundCall,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  CURRENT_DATE() ELSE  T.ETLUpdateTS END ) AS ETLUpdateTS,

 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.ETLHashbytes ELSE  T.ETLHashbytes END ) AS ETLHashbytes,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.CreatedDate ELSE  T.CreatedDate END ) AS CreatedDate,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.OutreachId ELSE  T.OutreachId END ) AS OutreachId,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.UserKey ELSE  T.UserKey END ) AS UserKey,

 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.AccessMethodKey ELSE  T.AccessMethodKey END ) AS AccessMethodKey,
 ( CASE WHEN S.PhoneCallHistoryId IS NOT NULL THEN  S.FactMemberAccessHistoryKey  ELSE  T.FactMemberAccessHistoryKey END ) AS FactMemberAccessHistoryKey

from  
fact_memberphonecallhistory_src T
Left Join 
{{ ref('int_censeonanalysis__phonecallhistory__src') }} S
on  T.ETLHashbytes != S.ETLHashbytes
AND T.FactMemberAccessHistoryKey = S.FactMemberAccessHistoryKey
AND s.PhoneCallHistoryId = 0
AND t.PhoneCallHistoryId = 0
),

phonecallhistory__src_int as (
select * from {{ ref('int_censeonanalysis__phonecallhistory__src') }}
),

update_phonecallhistory__src_int as 
(
    select 
    {{ dbt_utils.star(from=ref('int_censeonanalysis__phonecallhistory__src'), except=['IsInsert']) }},
    (Case when T.FactMemberAccessHistoryKey is not null then 0 else S.IsInsert end) as IsInsert
    from phonecallhistory__src_int S left join update_fact_memberphonecalhistory_1 T
    on T.FactMemberAccessHistoryKey = S.FactMemberAccessHistoryKey
      AND T.PhoneCallHistoryId = S.PhoneCallHistoryId
),

phonecallhistory__src as 
(
    select 
    {{ dbt_utils.star(from=ref('int_censeonanalysis__phonecallhistory__src'), except=['IsInsert']) }},
    (Case when IsInsert is null then 1 else IsInsert end) as IsInsert
    from update_phonecallhistory__src_int 
),

insert_fact_memberphonecalhistory as 
(
  SELECT
	S.FactMemberAccessHistoryKey, 
    S.PhoneCallHistoryId,
     S.CallCodeKey, S.CallDateKey, S.CallTimeKey, S.PhoneCallStatusId, S.MemberPhoneId,
    S.SchedulerUserKey, S.MemberPlanId, S.IsOutboundCall, 
    CURRENT_DATE() as ETLInsertTS , CURRENT_DATE() as ETLUpdateTS , S.ETLHashbytes
	, S.CreatedDate
	, S.OutreachId
	, S.UserKey
	, S.AccessMethodKey
    , FALSE as ISSCHEDULERGIVENCREDIT
    , FALSE as HASUCCXMATCH
	FROM phonecallhistory__src S
	WHERE S.IsInsert = 1

),

fact_memberphonecalhistory_union as 
(
    select * from update_fact_memberphonecalhistory_1
    union
    select * from insert_fact_memberphonecalhistory

),
phonecallhistory_to_delete as 
(
    SELECT t.FactMemberPhoneCallHistoryKey
    FROM fact_memberphonecalhistory_union T 
    LEFT JOIN phonecallhistory__src S
    ON T.FactMemberAccessHistoryKey = S.FactMemberAccessHistoryKey
    AND T.PhoneCallHistoryId = S.PhoneCallHistoryId
    WHERE T.PhoneCallHistoryId = 0 -- fake record inside of Target
    AND T.FactMemberAccessHistoryKey > 0
    AND T.CallDateKey >= 20191118	  --- :BackDateKey
    AND s.CreatedDate IS NULL 
),
fact_memberphonecalhistory as 
(
  select * from fact_memberphonecalhistory_union mph
  where FactMemberPhoneCallHistoryKey not in
   ( select FactMemberPhoneCallHistoryKey from phonecallhistory_to_delete )
  
)

select * from fact_memberphonecalhistory
