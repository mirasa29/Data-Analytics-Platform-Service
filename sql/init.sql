-- sql/init.sql

-- Drop tables if they exist to ensure a clean start on each run
DROP TABLE IF EXISTS transactions;
-- DROP TABLE IF EXISTS customers_dim; -- You might create these later in Spark
-- DROP TABLE IF EXISTS products_dim;
SET client_encoding = 'UTF8';

-- Create a table to hold the raw transaction data directly from the CSV
-- Adjust data types as needed based on your CSV's exact format
CREATE TABLE transactions (
    invoiceno VARCHAR(20),      -- Can be mixed with chars
    stockcode VARCHAR(20),
    description VARCHAR(255),
    quantity INTEGER,
    invoicedate TIMESTAMP,      -- Make sure format matches CSV
    unitprice NUMERIC(10, 4),
    customerid VARCHAR(20),         -- Some might be NULL in the dataset
    country VARCHAR(100)
);

-- Load data from the CSV file
-- Ensure the CSV file (e.g., 'data.csv') is in the /docker-entrypoint-initdb.d/ directory within the container
-- And that file permissions allow PostgreSQL to read it.
-- Specify DELIMITER, CSV, HEADER (if the first row is headers)
COPY transactions(invoiceno, stockcode, description, quantity, invoicedate, unitprice, customerid, country)
FROM '/docker-entrypoint-initdb.d/data.csv' -- Path within the Docker container
DELIMITER ','
CSV HEADER;

-- Optional: Add indexes for performance if needed for initial testing or direct queries
CREATE INDEX idx_transactions_customerid ON transactions (customerid);
CREATE INDEX idx_transactions_invoicedate ON transactions (invoicedate);

-- For your Python Ingestion Service, you can now read from this 'transactions' table.
-- Your Python extractor.py would query: SELECT * FROM transactions;
-- Or, if you need incremental loads based on 'invoicedate', you'd adapt the query:
-- SELECT * FROM transactions WHERE invoicedate > '{last_extracted_timestamp}'