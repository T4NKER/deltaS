-- Create seller database (marketplace database is created by POSTGRES_DB env var)
SELECT 'CREATE DATABASE seller_db OWNER deltasharing'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'seller_db')\gexec
