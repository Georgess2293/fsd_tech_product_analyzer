 CREATE OR REPLACE VIEW product_analyzer.reviews_sales AS (
 SELECT
    reviews.product_id,
	products.brand,
	products.model,
    reviews.review_date,
    reviews.total_reviews,
    reviews.positive_reviews,
    reviews.positive_percentage,
    reviews.negative_reviews,
    reviews.negative_percentage,
	sales.sales,
	sales.year
FROM product_analyzer.agg_reviews AS reviews
INNER JOIN product_analyzer.dim_product AS products
ON reviews.product_id=products.product_id
LEFT OUTER JOIN product_analyzer.fact_sales AS sales
ON products.brand=sales.brand AND EXTRACT(YEAR FROM reviews.review_date )=sales.year
)