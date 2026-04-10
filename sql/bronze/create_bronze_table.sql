CREATE TABLE IF NOT EXISTS bronze.users_raw (
    id INT,
    firstname VARCHAR(50),
    lastname VARCHAR(50),
    email VARCHAR(100),
    age INT,
    address SUPER,
    company SUPER,
    role VARCHAR(50),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);