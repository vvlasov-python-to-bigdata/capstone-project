SELECT
    campaignId, 
    sum(billingCost) as revenue 
FROM
    purchases 
WHERE
    isConfirmed = True 
GROUP BY 
    campaignId 
ORDER BY 
    revenue DESC 
LIMIT 10