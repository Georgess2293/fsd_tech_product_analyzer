CREATE TABLE IF NOT EXISTS product_analyzer.agg_reviews
(
    product_id INT,
    review_date DATE,
    total_reviews BIGINT,
    positive_reviews BIGINT,
    positive_percentage NUMERIC,
    negative_reviews BIGINT,
    negative_percentage NUMERIC,

);

INSERT INTO product_analyzer.agg_reviews
    (product_id,review_date,total_reviews,positive_reviews,positive_percentage,negative_reviews,negative_percentage)


WITH REVIEW_NUMBER AS(
SELECT
	products.product_id,
	reviews.review_date,
	COUNT(CASE WHEN
		reviews.sentiment='Positive' THEN reviews.review_id END) AS positive_reviews,
	COUNT(CASE WHEN
		reviews.sentiment='Negative' THEN reviews.review_id END) AS negative_reviews
FROM product_analyzer.fact_reviews as reviews
INNER JOIN product_analyzer.dim_product as products
ON reviews.product_id=products.product_id
GROUP BY
	products.product_id,
	reviews.review_date,
	products.release_date
ORDER BY reviews.review_date
	),

REVIEW_PERCENTAGES AS (
    SELECT
        product_id,
        review_date,
        positive_reviews+negative_reviews AS total_reviews,
        positive_reviews,
        negative_reviews,
        ROUND((positive_reviews * 100.0 /NULLIF(positive_reviews+negative_reviews,0)),1) AS positive_percentage,
        ROUND((negative_reviews * 100.0 /NULLIF(positive_reviews+negative_reviews,0)),1) AS negative_percentage
    FROM REVIEW_NUMBER
	)
	
SELECT
    product_id,
    review_date,
    total_reviews,
    positive_reviews,
    positive_percentage,
    negative_reviews,
    negative_percentage
FROM REVIEW_PERCENTAGES
ORDER BY review_date