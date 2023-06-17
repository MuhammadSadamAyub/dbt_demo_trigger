
with phone_callhistory_memeberaccess_hasoutreach_0 as (

   select 
  IFNULL(fo.FactMemberAccessHistoryKey,0) AS FactMemberAccessHistoryKey
, ph.IsOutbound AS IsOutboundCall
, fo.Resulting_CallCodeKey
, ph.PhoneCallHistoryId AS PhoneCallHistoryId
, CASE ph.PhoneCallStatusId
		WHEN 3 THEN 302
		WHEN 4 THEN 301
		WHEN 6 THEN 385
		WHEN 7 THEN 382
		WHEN 8 THEN 400
		WHEN 9 THEN 352
	END AS CallCodeId
, cast(TO_CHAR(ph.CreateDate, 'YYYYMMDD') as int) AS CallDateKey
, CAST(COALESCE(DATE_PART(HOUR, ph.CreateDate) * 60 * 60 + DATE_PART(MINUTE, ph.CreateDate) * 60 + DATE_PART(SECOND, ph.CreateDate) + 1, 1) AS INT) AS CallTimeKey
, ph.PhoneCallStatusId
, ph.MemberPhoneId
, ph.VirtusUserKey AS SchedulerUserKey
, ph.MemberPlanId AS MemberPlanId
, ph.CreateDate AS CreatedDate
, ph.OutreachId
, ph.UserKey AS UserKey
, fo.AccessMethodKey
, ROW_NUMBER() OVER (PARTITION BY ph.PhoneCallHistoryId	ORDER BY 
		  CASE when ph.CreateDate BETWEEN fo.AccessStart AND fo.AccessEnd THEN 0 ELSE 1 END ASC
		
		, DATEDIFF(SECOND, ph.CreateDate, fo.AccessEnd) ASC
		, ABS(DATEDIFF(SECOND, ph.CreateDate, fo.AccessEnd)) ASC		
		) as RN
        
FROM {{ ref('int_censeonanalysis__phonecallhistory') }} ph
INNER JOIN {{ ref('int_censeonanalysis__fact_memberhistory') }} fo -- for people with outreach, you only have 1 row per Outreach, Date, and User
ON ph.OutreachId = fo.OutreachId
	AND ph.CreateDateKey = fo.CallDateKey
	AND ph.UserKey = fo.UserKey	
WHERE ph.HasOutreach = 1

),

phone_callhistory_memeberaccess_filter_insert as 
(
select    
    FactMemberAccessHistoryKey,
    IsOutboundCall,
    Resulting_CallCodeKey,
    PhoneCallHistoryId,
    CallCodeId,
    CallDateKey,
    CallTimeKey,
    PhoneCallStatusId,
    MemberPhoneId,
    SchedulerUserKey,
    MemberPlanId,
    CreatedDate,
    OutreachId,
    UserKey,
	AccessMethodKey
     from phone_callhistory_memeberaccess_hasoutreach_0 where RN <=1
),

phone_callhistory_hasoutreach_1_insert as (

select 
   0 AS FactMemberAccessHistoryKey --IFNULL(fo.FactMemberAccessHistoryKey,0)
,  ph.IsOutbound AS IsOutboundCall
,  NULL AS Resulting_CallCodeKey-- fo.Resulting_CallCodeKey
,  ph.PhoneCallHistoryId AS PhoneCallHistoryId
,  CASE ph.PhoneCallStatusId
		WHEN 3 THEN 302
		WHEN 4 THEN 301
		WHEN 6 THEN 385
		WHEN 7 THEN 382
		WHEN 8 THEN 400
		WHEN 9 THEN 352
	END AS CallCodeId
, cast(TO_CHAR(ph.CreateDate, 'YYYYMMDD') as int) AS CallDateKey
, CAST(COALESCE(DATE_PART(HOUR, ph.CreateDate) * 60 * 60 + DATE_PART(MINUTE, ph.CreateDate) * 60 + DATE_PART(SECOND, ph.CreateDate) + 1, 1) AS INT) AS CallTimeKey
, ph.PhoneCallStatusId
, ph.MemberPhoneId
, ph.VirtusUserKey AS SchedulerUserKey
, ph.MemberPlanId AS MemberPlanId
, ph.CreateDate AS CreatedDate
, ph.OutreachId
, ph.UserKey
, 1 as AccessMethodKey
FROM {{ ref('int_censeonanalysis__phonecallhistory') }} ph
where ph.HasOutreach = 0

),

phonecall_history_insert as (

  select 
	fo.FactMemberAccessHistoryKey
, cast(case when UPPER(cc.CallCodeCategory) IN (
						  'SCHEDULED', 'DECLINED'
						, 'UNRESOLVED - VOICEMAIL'
						, 'UNRESOLVED - OTHER'
						, 'UNREACHED' ) then 1 
				WHEN am.AccessMethodId = 4
				THEN 1
				ELSE 0 end as NUMERIC) AS IsOutboundCall
, fo.Resulting_CallCodeKey
, 0 as PhoneCallHistoryId
, NULL as CallCodeId
, fo.CallDateKey AS CallDateKey
, fo.CallTimeKey AS CallTimeKey
, NULL as PhoneCallStatusId
, NULL as MemberPhoneId
, fo.SchedulerUserKey AS SchedulerUserKey
, fo.MemberPlanId
, fo.AccessStart
, fo.OutreachId
, fo.UserKey
, fo.AccessMethodKey
from {{ ref('int_censeonanalysis__fact_memberhistory') }} fo  

INNER JOIN {{ source('censeoanalysis', 'dimaccessmethod') }} am   ON am.AccessMethodKey = fo.AccessMethodKey
INNER JOIN {{ source('censeoanalysis', 'dim_user') }} du   ON du.UserKey = fo.UserKey
INNER JOIN {{ source('censeoanalysis', 'dim_date') }} dd   ON dd.DateKey = fo.CallDateKey
LEFT JOIN  {{ source('censeoanalysis', 'dim_callcode') }} cc on cc.CallCodeKey = fo.Resulting_CallCodeKey
			
-- NO PHONE CALL ASSOCIATED TO THE OUTREACH
LEFT JOIN {{ ref('int_censeonanalysis__phonecallhistory') }} pch 
ON pch.OutreachId = fo.OutreachId
AND pch.CreateDate >= dd.ActualDate
AND pch.CreateDate < DATEADD(DAY, 1, dd.ActualDate)
AND pch.UserId = du.UserID

-- NO PHONE CALL ASSOCIATED WHEN TIED TO SCHEDULER AND DATE RANGE
left join ( 
		SELECT pch.PhoneCallHistoryId
		, pch.IsOutbound
		, pch.CreateDate
		, pch.PhoneCallStatusId
		, pch.MemberPlanId
		, pch.VirtusUserKey
		, pch.MemberPhoneId
		, pch.OutreachId
		, pch.UserKey
		from {{ ref('int_censeonanalysis__phonecallhistory') }} pch
		where pch.HasOutreach = 0					  					  
	) ph
on ph.MemberPlanId = fo.MemberPlanId 
AND ph.VirtusUserKey = fo.SchedulerUserKey 
AND (ph.CreateDate between fo.AccessStart and fo.AccessEnd) 
AND ph.UserKey= fo.UserKey
			
WHERE ( ( fo.Resulting_CallCodeKey IS NOT NULL
			and UPPER(cc.CallCodeCategory) IN (
						  'SCHEDULED', 'DECLINED'
						, 'UNRESOLVED - VOICEMAIL'
						, 'UNRESOLVED - OTHER'
						, 'UNREACHED' ) 
		)
		OR
		am.AccessMethodId = 4 -- sometimes access methods 4 (dialer) have an access but no call... bring those in as undispositioned calls
		)			  
	and pch.PhoneCallHistoryId IS NULL -- NO PHONE CALL ASSOCIATED TO THE OUTREACH
	and ph.PhoneCallHistoryId IS NULL 

),

phone_callhistory_ui_final as 
(
  select * from  phone_callhistory_memeberaccess_filter_insert
  union
  select * from  phone_callhistory_hasoutreach_1_insert
  union
  select * from  phonecall_history_insert
)

select * from phone_callhistory_ui_final

 