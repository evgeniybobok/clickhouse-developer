SELECT
    town,
    sum(price) AS sum_price,
    formatReadableQuantity(sum_price)
FROM uk_price_paid
GROUP BY town
ORDER BY sum_price DESC;

-- to keep running aggregation, AggregateFunction is used (can be 'Simple' (min, max, sum) or not),
-- along with SummingMergeTree engine
CREATE OR REPLACE TABLE prices_sum_dest (
    town String,
    sum_price SimpleAggregateFunction(sum, UInt64) -- the value coming into SimpleAggregateFunction from sum() is UInt64
)
ENGINE = SummingMergeTree
PRIMARY KEY town;

-- create a view to dest with State function (can be 'Simple' as well)
DROP VIEW prices_sum_view;
CREATE MATERIALIZED VIEW prices_sum_view
TO prices_sum_dest AS
SELECT
    town,
    sumSimpleState(price) AS sum_price
FROM uk_price_paid
GROUP BY town;


-- populate prices_sum_dest as usual
INSERT INTO prices_sum_dest
SELECT
    town,
    sum(price) AS sum_price
FROM uk_price_paid
GROUP BY town;

-- check count
SELECT count()
FROM prices_sum_dest;
-- 1172

-- verify that the dest produces the same result as source
SELECT
    town,
    sum(price) AS sum_price,
    formatReadableQuantity(sum_price)
FROM uk_price_paid
WHERE town = 'LONDON'
GROUP BY town;
-- LONDON	1072797334445	1.07 trillion

SELECT
    town,
    sum_price AS sum,
    formatReadableQuantity(sum)
FROM prices_sum_dest
WHERE town = 'LONDON';
-- LONDON	1072797334445	1.07 trillion
-- looks good

-- now insert new rows into source
INSERT INTO uk_price_paid (price, date, town, street)
VALUES
    (4294967295, toDate('2024-01-01'), 'LONDON', 'My Street1');

-- now if we query the dest table, the result will be incorrect (actually, there are now 2 rows for LONDON)
SELECT
    town,
    sum_price AS sum,
    formatReadableQuantity(sum)
FROM prices_sum_dest
WHERE town = 'LONDON';
-- LONDON	4294967295	4.29 billion
-- LONDON	1072797334445	1.07 trillion

-- to fix the dest table, we need to use sum(sum_price) in the query
-- if the agg function is not 'sumple' (e.g. avg), then need to use avgMerge() function instead of avg()
SELECT
    town,
    sum(sum_price) AS sum,
    formatReadableQuantity(sum)
FROM prices_sum_dest
WHERE town = 'LONDON'
GROUP BY town;

-- get top 10 towns
SELECT
    town,
    sum(sum_price) AS sum
FROM prices_sum_dest
GROUP BY town
ORDER BY sum DESC
LIMIT 10;
-- LONDON	1077092301740
