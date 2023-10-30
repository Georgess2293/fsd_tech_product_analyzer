CREATE TABLE IF NOT EXISTS product_analyzer.dim_product
(
    product_id INT PRIMARY KEY,
    brand VARCHAR,
    model VARCHAR,
    release_date DATE
);
CREATE INDEX IF NOT EXISTS idx_product_id ON product_analyzer.dim_product (product_id);

INSERT INTO product_analyzer.dim_product
    (product_id,brand, model, release_date)
SELECT 
	src_product.product_id,
	src_product.brand,
	src_product.model,
	src_product.release_date
FROM product_analyzer.stg_products_specs src_product
ON CONFLICT (product_id)
DO UPDATE SET
    brand = EXCLUDED.brand,
    model = EXCLUDED.model,
    release_date = EXCLUDED.release_date;