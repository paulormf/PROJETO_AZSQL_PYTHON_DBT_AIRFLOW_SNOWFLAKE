SELECT
    customer_id, campaign_id, interaction_date,
    COUNT(*) AS duplicate_count
FROM {{ ref('silver_customer_campaigns') }}
GROUP BY customer_id, campaign_id, interaction_date
HAVING COUNT(*) > 1
