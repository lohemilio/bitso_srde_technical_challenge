-- 1. How many users were active on a given day (they made a deposit or withdrawal)
SELECT COUNT(DISTINCT user_id) AS active_users, DATE(event_timestamp) AS date
FROM transactions
GROUP BY DATE(event_timestamp);

-- 2. Identify users who haven't made a deposit
SELECT u.user_id
FROM users u
LEFT JOIN transactions t ON u.user_id = t.user_id AND t.transaction_type = 'deposit'
WHERE t.user_id IS NULL;

-- 3. Identify users who have made more than 5 deposits historically
SELECT user_id, COUNT(*) AS total_deposits
FROM transactions
WHERE transaction_type = 'deposit'
GROUP BY user_id
HAVING COUNT(*) > 5;

-- 4. When was the last time a user made a login
SELECT user_id, MAX(event_timestamp) AS last_login
FROM events
WHERE event_name = 'login'
GROUP BY user_id;
