
with source_phonecallhistory__phonecallhistoryIds as (

SELECT pch.PhoneCallHistoryId
FROM {{ source('censeoanalysis', 'phonecallhistory') }}  pch
INNER JOIN {{ source('censeoanalysis', 'memberplan') }}  mp ON mp.MemberPlanId = pch.MemberPlanId
WHERE CAST(pch.CreateDate AS DATE) >= CAST('{{ var("backdate") }}' AS DATE)
AND mp.PlanId != 37
),
phonecallhistory__phonecallhistory as 
(
SELECT pch.PhoneCallHistoryId
, ps.IsOutbound
, pch.CreateDate
, pch.PhoneCallStatusId
, pch.MemberPlanId
, vu.VirtusUserKey
, pch.MemberPhoneId
, pch.OutreachId
, du.UserKey
, CAST(0 AS NUMERIC) AS HasOutreach
, CAST(TO_CHAR(pch.CreateDate, 'YYYYMMDD') AS INT) AS CreateDateKey
, CAST(CAST(pch.CreateDate AS DATE) AS DATETIME) AS CreateStart
, DATEADD(DAY, 1, CAST(CAST(pch.CreateDate AS DATE) AS DATETIME)) AS CreateEnd
, NULL as UserId
FROM source_phonecallhistory__phonecallhistoryIds ii
INNER JOIN {{ source('censeoanalysis', 'phonecallhistory') }} pch  ON pch.PhoneCallHistoryId = ii.PhoneCallHistoryId
INNER JOIN {{ source('censeoanalysis', 'dim_virtususer') }} vu  ON vu.UserID = pch.UserId
INNER JOIN {{ source('censeoanalysis', 'phonecallstatus') }} ps  ON ps.PhoneCallStatusId = pch.PhoneCallStatusId
INNER JOIN {{ source('censeoanalysis', 'dim_user') }} du  ON du.UserId=pch.UserId 
AND pch.CreateDate BETWEEN du.StartDateTime AND du.EndDateTime
),
mch AS (
    SELECT *
    FROM {{ ref('int_censeonanalysis__fact_memberhistory') }}
),
mchmp AS (
    SELECT *
    FROM {{ ref('int_censeonanalysis__fact_memberhistory') }}
),
phonecallhistory__phonecallhistory_update_memberplan_id_hasoutreach as (
SELECT
    pch.PhoneCallHistoryId,
    pch.IsOutbound,
    pch.CreateDate,
    pch.PhoneCallStatusId,
    CASE
        WHEN mch.CallDateKey IS NOT NULL THEN 1
        ELSE pch.HasOutreach
    END AS HasOutreach,
    CASE
        WHEN mch.MemberPlanID IS NOT NULL THEN mch.MemberPlanID
        ELSE pch.MemberPlanId
    END AS MemberPlanId,
    pch.VirtusUserKey,
    pch.MemberPhoneId,
    pch.OutreachId,
    pch.UserKey,
    pch.CreateDateKey,
    pch.CreateStart,
    pch.CreateEnd,
    pch.UserId
FROM
    phonecallhistory__phonecallhistory pch
LEFT JOIN
    mch ON mch.CallDateKey = pch.CreateDateKey
        AND mch.OutreachId = pch.OutreachId
        AND mch.UserKey = pch.UserKey
LEFT JOIN
    mchmp ON mchmp.CallDateKey = pch.CreateDateKey
        AND mchmp.OutreachId = pch.OutreachId
        AND mchmp.UserKey = pch.UserKey
        AND mchmp.MemberPlanId = pch.MemberPlanId
        AND mchmp.FactMemberAccessHistoryKey IS NULL
),
phonecallhistory__phonecallhistory_update_hasoutreach as 
(

SELECT
    pch.PhoneCallHistoryId,
    pch.IsOutbound,
    pch.CreateDate,
    pch.PhoneCallStatusId,
    CASE
        WHEN mch.CallDateKey IS NOT NULL THEN 1
        ELSE pch.HasOutreach
    END AS HasOutreach,
    pch.MemberPlanId,
    pch.VirtusUserKey,
    pch.MemberPhoneId,
    pch.OutreachId,
    pch.UserKey,
    pch.CreateDateKey,
    pch.CreateStart,
    pch.CreateEnd,
    pch.UserId
FROM
    phonecallhistory__phonecallhistory_update_memberplan_id_hasoutreach pch
LEFT JOIN
    mch ON mch.CallDateKey = pch.CreateDateKey
        AND mch.OutreachId = pch.OutreachId
        AND mch.UserKey = pch.UserKey
        AND pch.HasOutreach = 0

),
phonecallhistory__phonecallhistory_update_userid as
(

SELECT
    pch.PhoneCallHistoryId,
    pch.IsOutbound,
    pch.CreateDate,
    pch.PhoneCallStatusId,
    pch.HasOutreach,
    pch.MemberPlanId,
    pch.VirtusUserKey,
    pch.MemberPhoneId,
    pch.OutreachId,
    pch.UserKey,
    pch.CreateDateKey,
    pch.CreateStart,
    pch.CreateEnd,
    CASE
        WHEN du.UserID IS NOT NULL THEN du.UserID
        ELSE pch.UserID
    END AS UserID
FROM
    phonecallhistory__phonecallhistory_update_hasoutreach pch
     LEFT JOIN
    {{ source('censeoanalysis', 'dim_user') }} du  ON du.UserKey = pch.UserKey
    
),
final as 
(

    select * from phonecallhistory__phonecallhistory_update_userid
)

select * from final 