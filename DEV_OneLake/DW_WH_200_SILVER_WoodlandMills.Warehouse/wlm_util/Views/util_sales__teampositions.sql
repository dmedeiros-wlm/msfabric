create view "wlm_util"."util_sales__teampositions" as with
    src as (
        SELECT 'CX Manager' AS [teamposition], '7f738a5d-3d9a-ec11-b401-0022486df72b' AS [userid] 
        UNION ALL
        SELECT 'CX Project Coordinator', 'f6feadeb-cbbf-ee11-9078-6045bd60d07e' 
        UNION ALL
        SELECT 'Product Advisor', 'ad9c353e-6f7d-ec11-8d21-000d3af4da5f'
        UNION ALL
        SELECT 'Product Advisor', '124c0f8c-a62d-ef11-8e50-6045bd615605'
        UNION ALL
        SELECT 'Product Advisor', 'ca35801e-6689-ec11-93b0-0022486d6541'
        UNION ALL
        SELECT 'Product Advisor', '27d2781f-d8ba-ee11-9078-0022483ccb4c'
        UNION ALL
        SELECT 'Product Advisor', 'c5293fb4-a922-ef11-840a-0022483ea594'
        UNION ALL
        SELECT 'Product Advisor', 'e32ad31b-f2ba-ee11-9079-002248b0ad37'
        UNION ALL
        SELECT 'Product Advisor', 'c49c353e-6f7d-ec11-8d21-000d3af4da5f'
        UNION ALL
        SELECT 'Product Advisor', '853ec498-f614-ee11-9cbe-000d3a09d7c4'
    )

select *
from src;