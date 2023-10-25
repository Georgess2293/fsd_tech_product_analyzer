CREATE TABLE IF NOT EXISTS product_analyzer.fact_sales
(
    sales_id SERIAL PRIMARY KEY NOT NULL,
    brand VARCHAR,
    sales FLOAT,
    year INT
);
CREATE INDEX IF NOT EXISTS idx_sales_id ON product_analyzer.fact_sales (sales_id);

-- INSERT INTO product_analyzer.fact_reviews
--     (brand,sales,year)
-- SELECT 
-- 	src_sales.brand,
-- 	src_sales.sales,
--     CAST(src_sales.year AS INT) AS year
-- FROM product_analyzer.stg_sales as src_sales