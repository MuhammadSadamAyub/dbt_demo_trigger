with source_fact_memberaccesshistory as (

    select * 
    from {{ ref('stg_censeonanalysis__fact_memberhistory') }}  

)
fact_memberphonecalhistory as 
(

    select 
    {{ dbt_utils.star(from=ref('stg_censeonanalysis__fact_memberhistory'), except=['Resulting_CallCodeKey']) }},
    (CASE when mah.Resulting_CallCodeKey != fpc.CallCodeKey then fpc.CallCodeKey else mah.Resulting_CallCodeKey end ) as Resulting_CallCodeKey   
    from 
    source_fact_memberaccesshistory mah
    Left JOIN  {{ ref('fact_memberphonecallhistory') }}   fpc
    On mah.FactMemberAccessHistoryKey = fpc.FactMemberAccessHistoryKey
	AND mah.Resulting_CallCodeKey != fpc.CallCodeKey  
    LEFT JOIN 
   {{ source('censeoanalysis', 'dim_callcode') }}  cc ON cc.CallCodeKey = mah.Resulting_CallCodeKey ,
    LEFT JOIN 
    {{ source('censeoanalysis', 'dimaccessmethod') }}  am ON  am.AccessMethodKey = mah.AccessMethodKey
    AND am.AccessMethodId = 4
	WHERE 
	 cc.CallCodeID IN ( 389, -1, -2 )
	AND fpc.CallDateKey >= 20191118	  --- :BackDateKey

)
select * from fact_memberphonecalhistory