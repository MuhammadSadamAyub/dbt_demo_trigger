with source_fact_memberaccesshistory as (

    select * 
    from {{ ref('stg_censeonanalysis__fact_memberhistory') }}  

),
fact_memberphonecalhistory as 
(

    select 
 mah."MEMBERCALLHISTORYID",
mah."NEXTACCESSMEMBERCALLHISTORYID",
mah."CALLDATEKEY",
mah."CALLTIMEKEY",
mah."RESULTINGMEMBERCALLHISTORYID",
mah."MEMBERKEY",
mah."PLANKEY",
mah."SCHEDULERUSERKEY",
mah."ETLINSERTTS",
mah."ETLUPDATETS",
mah."ETLHASHBYTES",
mah."APPOINTMENTKEY",
mah."ACCESSSTART",
mah."ACCESSEND",
mah."MEMBERPLANID",
mah."HASUCCXMATCH",
mah."ISFIRSTACCESS",
mah."ISFIRSTACCESSRESOLVED",
mah."ISSCHEDULERGIVENCREDIT",
mah."CONTACTPURPOSE",
mah."DIMMEMBERPHONECALLHISTORYKEY",
mah."OUTREACHID",
mah."USERKEY",
mah."APPOINTMENTVERSIONID",
mah.FactMemberAccessHistoryKey,
mah.AccessMethodKey,
    (CASE when mah.Resulting_CallCodeKey != fpc.CallCodeKey then fpc.CallCodeKey else mah.Resulting_CallCodeKey end ) as Resulting_CallCodeKey   
    from 
    source_fact_memberaccesshistory mah
    Left JOIN  {{ ref('fact_memberphonecallhistory') }}   fpc
    On mah.FactMemberAccessHistoryKey = fpc.FactMemberAccessHistoryKey
	AND mah.Resulting_CallCodeKey != fpc.CallCodeKey  
    LEFT JOIN 
   {{ source('censeoanalysis', 'dim_callcode') }}  cc ON cc.CallCodeKey = mah.Resulting_CallCodeKey 
    LEFT JOIN 
    {{ source('censeoanalysis', 'dimaccessmethod') }}  am ON  am.AccessMethodKey = mah.AccessMethodKey
    AND am.AccessMethodId = 4
	WHERE 
	 cc.CallCodeID IN ( 389, -1, -2 )
	AND fpc.CallDateKey >= {{ var("backdatekey") }} 	  --- :BackDateKey

)
select * from fact_memberphonecalhistory