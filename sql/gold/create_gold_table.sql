CREATE TABLE IF NOT EXISTS gold.users_by_state (
    state VARCHAR(50),
    user_count INT,
    avg_age FLOAT,
    youngest_user INT,
    oldest_user INT,
    updated_at TIMESTAMP
);