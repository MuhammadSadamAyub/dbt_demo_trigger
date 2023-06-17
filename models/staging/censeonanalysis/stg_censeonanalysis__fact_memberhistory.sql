
with source_fact_memberaccesshistory as (
    select *
    from {{ source('censeoanalysis', 'fact_memberaccesshistory') }} mx 
),

final as (
select *
from source_fact_memberaccesshistory
)

select * from final 
 