SELECT formatReadableQuantity(count())
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet');


SELECT *
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
LIMIT 10;


SELECT crypto_name, avg(volume) as avg_volume
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
WHERE crypto_name = 'Bitcoin'
GROUP BY crypto_name;
-- 10411891574.238258


SELECT trim(crypto_name), count() as count
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
GROUP BY crypto_name
ORDER BY crypto_name DESC
LIMIT 10;
-- zzz.finance         │    75


SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet')
LIMIT 10;


SELECT count()
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');


CREATE TABLE pypi
(
    TIMESTAMP DateTime64(3, 'UTC'),
    COUNTRY_CODE String,
    URL String,
    PROJECT String
)
ENGINE = MergeTree
ORDER BY (TIMESTAMP);


INSERT INTO pypi
SELECT
    TIMESTAMP,
    COUNTRY_CODE,
    URL,
    PROJECT
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');


SELECT
    PROJECT,
    count()
FROM pypi
WHERE toStartOfMonth(TIMESTAMP) = '2023-04-01'
GROUP BY PROJECT
ORDER BY count() DESC
LIMIT 10;
-- 557,056 rows


SELECT
    PROJECT,
    count()
FROM pypi
WHERE PROJECT LIKE 'boto%'
GROUP BY PROJECT
ORDER BY count() DESC
LIMIT 10;
-- 557,056 rows


-- create pypti2 table with primary key on PROJECT and TIMESTAMP
DROP TABLE IF EXISTS pypi2;
CREATE OR REPLACE TABLE pypi2
(
    `TIMESTAMP` DateTime,
    `COUNTRY_CODE` String,
    `URL` String,
    `PROJECT` String
)
ENGINE = MergeTree
PRIMARY KEY (PROJECT, TIMESTAMP);


INSERT INTO pypi2
SELECT *
FROM pypi;


-- check the size of the table
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table LIKE '%pypi%')
GROUP BY table;


CREATE OR REPLACE TABLE test_pypi
(
    `TIMESTAMP` DateTime,
    `COUNTRY_CODE` String,
    `URL` String,
    `PROJECT` String
)
ENGINE = MergeTree
PRIMARY KEY (PROJECT, COUNTRY_CODE, TIMESTAMP);


INSERT INTO test_pypi
SELECT *
FROM pypi;


SELECT uniqExact(URL)
FROM pypi;


SELECT count(DISTINCT COUNTRY_CODE)
FROM pypi;


-- create a table with LowCardinality on COUNTRY_CODE and PROJECT
DROP TABLE IF EXISTS pypi3;
CREATE TABLE pypi3
(
    TIMESTAMP DateTime64(3, 'UTC'),
    COUNTRY_CODE LowCardinality(String),
    URL String,
    PROJECT LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (PROJECT, TIMESTAMP);

INSERT INTO pypi3
SELECT *
FROM pypi;


-- check the size of the tables
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table LIKE 'pypi%')
GROUP BY table;


SELECT
    toStartOfMonth(TIMESTAMP) AS month,
    count() AS count
FROM pypi2
WHERE COUNTRY_CODE = 'US'
GROUP BY
    month
ORDER BY
    month ASC,
    count DESC;
-- 0.017 seconds
-- 0.051
18
19
18

SELECT
    toStartOfMonth(TIMESTAMP) AS month,
    count() AS count
FROM pypi3
WHERE COUNTRY_CODE = 'US'
GROUP BY
    month
ORDER BY
    month ASC,
    count DESC;
-- 0.071 seconds
0.045
63
33
18
17


DROP TABLE IF EXISTS crypto_prices;
CREATE OR REPLACE TABLE crypto_prices
(
    trade_date Date,
    crypto_name LowCardinality(String),
    volume Float32,
    price Float32,
    market_cap Float32,
    change_1_day Float32
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name, trade_date);


INSERT INTO crypto_prices
SELECT *
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet');


SELECT count()
FROM crypto_prices
WHERE volume >= 1000_000;
-- 2,382,643 rows read

SELECT avg(price)
FROM crypto_prices
WHERE crypto_name like 'B%';
-- 24,576 rows read
-- 237,568 rows read - the index also helps


CREATE TABLE uk_price_paid (
    price	UInt32,
    date	Date,
    postcode1	LowCardinality(String),
    postcode2	LowCardinality(String),
    type	Enum('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
    is_new	UInt8,
    duration	Enum('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
    addr1	String,
    addr2	String,
    street	LowCardinality(String),
    locality	LowCardinality(String),
    town	LowCardinality(String),
    district	LowCardinality(String),
    county	LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, date);

INSERT INTO uk_price_paid
SELECT *
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices.snappy.parquet');

SELECT avg(price)
FROM uk_price_paid
WHERE postcode1='LU1' AND postcode2='5FT';
-- only 7 granules read
--73461.87334593573

SELECT avg(price)
FROM uk_price_paid
WHERE postcode2='5FT';
-- 23,558,625 rows read because the postcode1 index is not used and it reads all parts with skipping only a few granules

SELECT avg(price)
FROM uk_price_paid
WHERE town='YORK';
-- reads all rows because no index is used
-- 208573.36750813763


SET format_csv_delimiter = '~'; -- or SETTINGS format_csv_delimiter='~' at the end of the query

SELECT count()
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv');
-- 6205

SELECT formatReadableQuantity(sum(actual_amount))
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS format_csv_delimiter = '~';
-- 8163564603.14
-- 8.16 billion


SELECT
    formatReadableQuantity(sum(toUInt32OrZero(approved_amount))),
    formatReadableQuantity(sum(toUInt32OrZero(recommended_amount)))
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS format_csv_delimiter='~';


DESC s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS format_csv_delimiter='~',
schema_inference_make_columns_nullable=false;


CREATE TABLE operating_budget
(
    fiscal_year	LowCardinality(String),
    service	LowCardinality(String),
    department	LowCardinality(String),
    program	LowCardinality(String),
    program_code    LowCardinality(String),
    description	String,
    item_category	LowCardinality(String),
    approved_amount	UInt32,
    recommended_amount	UInt32,
    actual_amount	Decimal(12,2),
    fund	LowCardinality(String),
    fund_type	Enum('GENERAL FUNDS'=1, 'FEDERAL FUNDS'=2, 'OTHER FUNDS'=3)
)
ENGINE = MergeTree
ORDER BY (fiscal_year, program);


INSERT INTO operating_budget
SELECT
    fiscal_year,
    service,
    department,
    program,
    program_code,
    description,
    item_category,
    toUInt32OrZero(approved_amount),
    toUInt32OrZero(recommended_amount),
    toDecimal64(actual_amount, 2),
    fund,
    fund_type
FROM s3(
    'https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv',
    format_csv_delimiter='~',
    input_format_csv_skip_first_lines=1
)
SETTINGS format_csv_delimiter='~', input_format_csv_skip_first_lines=1;



-- ingestion
-- intest via temp table
CREATE OR REPLACE TABLE operating_budget_s3
(
    t_fiscal_year	String,
    t_service	String,
    t_department	String,
    t_program	String,
    t_description	String,
    t_item_category	String,
    t_approved_amount	String,
    t_recommended_amount	String,
    t_actual_amount	Float64,
    t_fund	String,
    t_fund_type	String
)
ENGINE = S3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv', 'CSV')
SETTINGS format_csv_delimiter='~', input_format_csv_skip_first_lines=1;

-- insert into a target table with proper types
INSERT INTO operating_budget
SELECT
    t_fiscal_year as fiscal_year,
    t_service as service,
    t_department as department,
    trim(splitByChar('(', t_program)[1]) as program,
    replaceOne(splitByChar('(', t_program)[2], ')', '') as program_code,
    t_description as description,
    t_item_category as item_category,
    toUInt32OrZero(t_approved_amount) as approved_amount,
    toUInt32OrZero(t_recommended_amount) as recommended_amount,
    toDecimal64(t_actual_amount, 2) as actual_amount,
    t_fund as fund,
    t_fund_type as fund_type
FROM operating_budget_s3;


SELECT formatReadableQuantity(sum(approved_amount))
FROM operating_budget
WHERE fiscal_year='2022';
-- 5086410509
-- 5.09 billion

SELECT sum(actual_amount)
FROM operating_budget
WHERE fiscal_year='2022' AND program_code='031';


SELECT *
FROM uk_price_paid
WHERE price > 100_000_000
ORDER BY price DESC;


SELECT count()
FROM uk_price_paid
WHERE price > 1_000_000 AND toStartOfYear(date) = '2022-01-01';
-- 36475

SELECT count(DISTINCT town)
FROM uk_price_paid;
-- 1172

SELECT town, count() c
FROM uk_price_paid
GROUP BY 1
ORDER BY c DESC
LIMIT 1;
-- London 2188031

SELECT topK(10)(town)
FROM uk_price_paid
WHERE town != 'LONDON';
-- [
--   "MANCHESTER",
--   "BIRMINGHAM",
--   "BRISTOL",
--   "LEEDS",
--   "NOTTINGHAM",
--   "SHEFFIELD",
--   "LEICESTER",
--   "LIVERPOOL",
--   "SOUTHAMPTON",
--   "NEWCASTLE UPON TYNE"
-- ]

SELECT town, avg(price)
FROM uk_price_paid
GROUP BY town
ORDER BY avg(price) DESC
LIMIT 10;


SELECT town, avg(price) as avg_pr
FROM uk_price_paid
GROUP BY town
ORDER BY avg_pr DESC
LIMIT 10;

with ranked as(
SELECT *, row_number() OVER (ORDER BY price DESC) as rn
FROM uk_price_paid
)
SELECT *
FROM ranked
WHERE rn = 1;

SELECT type, avg(price)
FROM uk_price_paid
GROUP BY type;


SELECT sum(price)
FROM uk_price_paid
WHERE initcap(county) IN ('Avon', 'Essex', 'Devon', 'Kent', 'Cornwall')
AND toStartOfYear(date) = '2020-01-01';
--29935920858


with pre as (
    SELECT *,
    max(price) OVER (ORDER BY price DESC) as max_price,
    row_number() OVER (PARTITION BY town ORDER BY price DESC) as rn_town
    FROM uk_price_paid
)
SELECT town, price/max_price as price_ratio
FROM pre
WHERE rn_town = 1
ORDER BY price_ratio DESC;


CREATE VIEW london_properties_view
AS
    SELECT date, price, addr1, addr2, street
    FROM uk_price_paid
    WHERE town='LONDON';

SELECT avg(price)
FROM london_properties_view
WHERE toStartOfYear(date)='2022-01-01';


SELECT count()
FROM london_properties_view;
--2188031


CREATE VIEW properties_by_town_view
AS
    SELECT date, price, addr1, addr2, street
    FROM uk_price_paid
    WHERE town={town:String};


with pre as (
SELECT *,
row_number() OVER (ORDER BY price DESC) as rn
FROM properties_by_town_view(town='LIVERPOOL'))
SELECT price, street
FROM pre
WHERE rn=1;
-- SEFTON STREET

-- or ClickHouse way
SELECT
    argMax(street, price) as street,
    max(price) as max_price
FROM properties_by_town_view(town='LIVERPOOL');


SELECT
    count() cnt,
    avg(price) avp
FROM uk_price_paid
WHERE toStartOfYear(date) = '2020-01-01';
--886642
--378060.000030452
-- 28,634,236 rows read


SELECT
    toYear(date) yr,
    count() cnt,
    avg(price) avp
FROM uk_price_paid
GROUP BY yr
ORDER BY yr DESC;

-- create table ordered (indexed) by town and date
CREATE TABLE prices_by_year_dest (
    date Date,
    price UInt32,
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (town, date)
PARTITION BY toYear(date);

-- create a materialized view for populating prices_by_year_dest
CREATE MATERIALIZED VIEW prices_by_year_view
TO prices_by_year_dest
AS
    SELECT date, price, addr1, addr2, street, town, district, county
    FROM uk_price_paid;

-- populate the table from original data
INSERT INTO prices_by_year_dest
SELECT date, price, addr1, addr2, street, town, district, county
FROM uk_price_paid;


-- check the size is the same
SELECT count()
FROM prices_by_year_dest;

-- check table parts
SELECT * FROM system.parts
WHERE table='prices_by_year_dest';
-- lots of parts! Which may cause performance issues

-- check parts for uk_price_paid
SELECT * FROM system.parts
WHERE table='uk_price_paid';
-- inly 2 parts which is better for performance


SELECT
    count() cnt,
    avg(price) avp
FROM prices_by_year_dest
WHERE toStartOfYear(date) = '2020-01-01';
-- 886,642 rows read compared to 28,634,236 rows read from uk_price_paid


SELECT
    count() cnt,
    max(price) maxp,
    avg(price) avp,
    quantile(0.90)(price) q90
FROM prices_by_year_dest
WHERE toStartOfMonth(date) = '2005-06-01'
AND initcap(county) = 'Staffordshire';
-- cnt:  1322
-- maxp: 745000
-- avp:  160241.94402420576
-- q90:  269670.00000000006

-- insert 3 rows into uk_price_paid
INSERT INTO uk_price_paid VALUES
    (125000, '2024-03-07', 'B77', '4JT', 'semi-detached', 0, 'freehold', 10,'',	'CRIGDON','WILNECOTE','TAMWORTH','TAMWORTH','STAFFORDSHIRE'),
    (440000000, '2024-07-29', 'WC1B', '4JB', 'other', 0, 'freehold', 'VICTORIA HOUSE', '', 'SOUTHAMPTON ROW', '','LONDON','CAMDEN', 'GREATER LONDON'),
    (2000000, '2024-01-22','BS40', '5QL', 'detached', 0, 'freehold', 'WEBBSBROOK HOUSE','', 'SILVER STREET', 'WRINGTON', 'BRISTOL', 'NORTH SOMERSET', 'NORTH SOMERSET');

-- check if they appered in prices_by_year_dest
SELECT *
FROM prices_by_year_dest
WHERE toYear(date) = 2024;
-- yes

-- and check the new part
SELECT * FROM system.parts
WHERE table='prices_by_year_dest';
-- yes
