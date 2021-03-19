SELECT 
    campaignId, 
    channelId, 
    sessionsCount, 
    RANK() OVER (PARTITION BY campaignId ORDER BY sessionsCount DESC) AS channelRank 
FROM 
    (
        SELECT 
            campaignId, 
            channelId, 
            COUNT(DISTINCT sessionId) as sessionsCount
        FROM purchases 
        GROUP BY campaignId, channelId 
    )