COPY bronze.users_raw
FROM 's3://ecommerce-lakehousev2/bronze/users/'
IAM_ROLE 'arn:aws:iam::590807097292:role/RedshiftS3Role'
FORMAT AS JSON 'auto ignorecase';