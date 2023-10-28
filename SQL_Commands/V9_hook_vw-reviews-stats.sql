CREATE OR REPLACE VIEW product_analyzer.reviews_stats AS (
    SELECT
    reviews.product_id,
	products.brand,
	products.model,
    reviews.review_date,
    reviews.total_reviews,
    reviews.positive_reviews,
    reviews.positive_percentage,
    reviews.negative_reviews,
    reviews.negative_percentage
FROM product_analyzer.agg_reviews AS reviews
INNER JOIN product_analyzer.dim_product AS products
ON reviews.product_id=products.product_id
)