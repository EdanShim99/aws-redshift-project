CREATE TABLE IF NOT EXISTS silver.users (
    id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    age INT,
    state VARCHAR(50),
    department VARCHAR(100),
    company_name VARCHAR(100),
    role VARCHAR(50),
    updated_at TIMESTAMP
);