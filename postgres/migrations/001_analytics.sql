\getenv aggregation_password AGGREGATION_POSTGRES_PASSWORD

SELECT 'CREATE ROLE aggregation'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'aggregation')
\gexec

ALTER ROLE aggregation LOGIN PASSWORD :'aggregation_password';
CREATE SCHEMA IF NOT EXISTS analytics AUTHORIZATION aggregation;
