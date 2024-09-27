SELECT c.username as creator,  oc.creatorposition, o.username as owner, oc.ownerposition, a.username as activeagent, oc.activeagentposition, count(*) as no_events
FROM [wlm_int].[int_omnichannel_joined] oc
left join [wlm_stg].[stg_dataverse__systemusers] o on oc.ownerid = o.userid
left join [wlm_stg].[stg_dataverse__systemusers] c on oc.createdbyuserid = c.userid
left join [wlm_stg].[stg_dataverse__systemusers] a on oc.activeagentid = a.userid
where year(oc.createdondate) = 2024
group by c.username, o.username, oc.creatorposition, oc.ownerposition, a.username, oc.activeagentposition;
