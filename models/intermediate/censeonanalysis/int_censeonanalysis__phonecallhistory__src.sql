
with phonecallhistory_src as 
(

select
	  y.FactMemberAccessHistoryKey
	, y.PhoneCallHistoryId
	, y.CallCodeKey
	, y.CallDateKey
	, y.CallTimeKey
	, y.PhoneCallStatusId
	, y.MemberPhoneId
	, y.SchedulerUserKey
	, y.MemberPlanId
	, y.IsOutboundCall 
	, y.CreatedDate
	, y.OutreachId
	, y.UserKey
	, TO_BINARY
		(SHA1(
			  IFNULL(CAST(y.CallCodeKey AS VARCHAR(500)), '####') || '|'
			|| IFNULL(CAST(y.CallDateKey AS VARCHAR(500)), '####') || '|'
			|| IFNULL(CAST(y.CallTimeKey AS VARCHAR(500)), '####') || '|'
			|| IFNULL(CAST(y.PhoneCallStatusId AS VARCHAR(500)), '####') || '|'
			|| IFNULL(CAST(y.MemberPhoneId AS VARCHAR(500)), '####') || '|'
			|| IFNULL(CAST(y.SchedulerUserKey AS VARCHAR(500)), '####') || '|'
			|| IFNULL(CAST(y.UserKey AS VARCHAR(500)), '####') || '|'
			|| IFNULL(CAST(y.MemberPlanId AS VARCHAR(500)), '####') || '|'
			|| IFNULL(CAST(y.IsOutboundCall AS VARCHAR(500)), '####') || '|' 
			|| IFNULL(CAST(y.CreatedDate AS VARCHAR(500)), '####') 	
			|| IFNULL(CAST(y.OutreachId AS VARCHAR(500)), '####') 							
			) ) AS ETLHashbytes
	, CAST(NULL AS INT) AS UserId
	, CAST(NULL AS VARCHAR(25)) AS PhoneNumber
	, CAST(0 AS NUMERIC) AS ToDelete
	, y.AccessMethodId AS AccessMethodId
	, AccessMethodKey
	--, 0
	from ( 
		select 
		x.FactMemberAccessHistoryKey
		, x.PhoneCallHistoryId
		, COALESCE(cc.CallCodeKey, x.Resulting_CallCodeKey, (SELECT cc.CallCodeKey FROM {{ source('censeoanalysis', 'dim_callcode') }} WHERE cc.CallCodeID = -1) ) AS CallCodeKey
		, x.CallDateKey
		, x.CallTimeKey
		, x.PhoneCallStatusId
		, x.MemberPhoneId
		, x.SchedulerUserKey
		, x.MemberPlanId
		, x.IsOutboundCall
		, x.CreatedDate
		, x.OutreachId
		, x.UserKey
		, dam.AccessMethodId
		, x.AccessMethodKey
		from {{ ref('int_censeonanalysis__phonecallhistory__u1') }} x
		left join {{ source('censeoanalysis', 'dim_callcode') }} cc   ON cc.CallCodeID = x.CallCodeId
		LEFT JOIN {{ source('censeoanalysis', 'dimaccessmethod') }} dam   ON dam.AccessMethodKey = x.AccessMethodKey
	) y



),

phonecallhistory_src_final as 
(
    select * from phonecallhistory_src
)

select * from phonecallhistory_src_final