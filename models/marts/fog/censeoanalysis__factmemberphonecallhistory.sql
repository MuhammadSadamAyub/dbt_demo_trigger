with censeoanalysis__factmemberphonecallhistory as 
(

    select * from {{ ref('fact_memberphonecallhistory') }} where 
    WHERE ETLUpdateTS >= CURRENT_DATE()  --:ThisRun
)

select * from censeoanalysis__factmemberphonecallhistory