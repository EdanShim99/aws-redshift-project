DELETE FROM gold.users_by_state;

INSERT INTO gold.users_by_state
SELECT
    state,
    COUNT(*) AS user_count,
    AVG(age) AS avg_age,
    MIN(age) AS youngest_user,
    MAX(age) AS oldest_user,
    CURRENT_TIMESTAMP
FROM silver.users
GROUP BY state;