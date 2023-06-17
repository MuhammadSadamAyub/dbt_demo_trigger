
with source_fact_memberphonecallhistory as (

    select
    *
    from {{ source('censeoanalysis', 'fact_memberphonecallhistory') }} 

),

final as (
select * from source_fact_memberphonecallhistory
)

select * from final 
 