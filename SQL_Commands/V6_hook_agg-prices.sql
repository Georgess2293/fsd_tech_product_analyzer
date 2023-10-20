CREATE TABLE IF NOT EXISTS product_analyzer.agg_prices
(
    product_id,
    avg_price
);

INSERT INTO product_analyzer.agg_prices
    (product_id,avg_price)

SELECT product_id,
       ROUND((
           COALESCE(_128gb_8gb_ram, 0) + COALESCE(_128gb_6gb_ram, 0) + COALESCE(_256gb_6gb_ram, 0) +
           COALESCE(_128gb_12gb_ram, 0) + COALESCE(_512gb_16gb_ram, 0) + COALESCE(_256gb_12gb_ram, 0) +
           COALESCE(_32gb_3gb_ram, 0) + COALESCE(_64gb_4gb_ram, 0) + COALESCE(_128gb_4gb_ram, 0) +
           COALESCE(_512gb_12gb_ram, 0) + COALESCE(_64gb_6gb_ram, 0) + COALESCE(_256gb_6gb_ram, 0) +
           COALESCE(_512gb_6gb_ram, 0) + COALESCE(_256gb_4gb_ram, 0) + COALESCE(_512gb_4gb_ram, 0) +
           COALESCE(_512gb_8gb_ram, 0)
       ) /
       NULLIF(
           (
               CASE WHEN _128gb_8gb_ram IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN _128gb_6gb_ram IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN _256gb_6gb_ram IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN _128gb_12gb_ram IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN _512gb_16gb_ram IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN _256gb_12gb_ram IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN _32gb_3gb_ram IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN _64gb_4gb_ram IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN _128gb_4gb_ram IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN _512gb_12gb_ram IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN _64gb_6gb_ram IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN _256gb_6gb_ram IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN _512gb_6gb_ram IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN _256gb_4gb_ram IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN _512gb_4gb_ram IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN _512gb_8gb_ram IS NOT NULL THEN 1 ELSE 0 END
           ), 0)
       ) AS avg_price
FROM product_analyzer.dim_prices