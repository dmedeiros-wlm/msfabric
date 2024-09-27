SELECT c.username as creator,  oc.creatorposition, o.username as owner, oc.ownerposition, count(*) as no_events
FROM [wlm_int].[int_leads_joined] oc
left join [wlm_stg].[stg_dataverse__systemusers] o on oc.ownerid = o.userid
left join [wlm_stg].[stg_dataverse__systemusers] c on oc.createdbyuserid = c.userid
where year(oc.createdondate) = 2024
group by c.username, o.username, oc.creatorposition, oc.ownerposition;