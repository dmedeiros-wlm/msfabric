create view "wlm_util"."util_techsupport__teampositions" as with
    src as (
        SELECT 'Team Lead' AS [teamposition], 'c435801e-6689-ec11-93b0-0022486d6541' AS [userid] 
        UNION ALL
        SELECT 'Senior Tech', 'e1523d38-6f7d-ec11-8d21-000d3af4da5f' 
        UNION ALL
        SELECT 'Senior Tech', 'db9c353e-6f7d-ec11-8d21-000d3af4da5f'
        UNION ALL
        SELECT 'Tech Advisor', '30223e8f-f7ba-ee11-9079-002248b0ab65'
        UNION ALL
        SELECT 'Tech Advisor', '37dadbf3-2eda-ee11-904d-002248b0a984'
        UNION ALL
        SELECT 'Tech Advisor', 'cfdfc561-e517-ed11-b83e-000d3af4fa76'

    )

select *
from src;