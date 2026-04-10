MERGE INTO silver.users
USING (
    SELECT *
    FROM (
        SELECT
            id,
            firstname AS first_name,
            lastname AS last_name,
            email,
            age,
            address.state::VARCHAR AS state,
            company.department::VARCHAR AS department,
            company.name::VARCHAR AS company_name,
            role,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY ingestion_timestamp DESC
            ) AS rn
        FROM bronze.users_raw
    ) ranked
    WHERE rn = 1
) s
ON silver.users.id = s.id

WHEN MATCHED THEN UPDATE SET
    first_name = s.first_name,
    last_name = s.last_name,
    email = s.email,
    age = s.age,
    state = s.state,
    department = s.department,
    company_name = s.company_name,
    role = s.role,
    updated_at = CURRENT_TIMESTAMP

WHEN NOT MATCHED THEN INSERT (
    id,
    first_name,
    last_name,
    email,
    age,
    state,
    department,
    company_name,
    role,
    updated_at
)
VALUES (
    s.id,
    s.first_name,
    s.last_name,
    s.email,
    s.age,
    s.state,
    s.department,
    s.company_name,
    s.role,
    CURRENT_TIMESTAMP
);