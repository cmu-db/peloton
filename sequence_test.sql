-- psql "sslmode=disable" -U postgres -h localhost -p 15721
\c test;
-- currently not support AS, CACHE and OWNED BY.
CREATE SEQUENCE seq INCREMENT BY 2 MINVALUE 10 MAXVALUE 50 START 10 CYCLE;
