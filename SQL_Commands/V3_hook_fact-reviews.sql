CREATE TABLE IF NOT EXISTS product_analyzer.fact_reviews
(
    review_id SERIAL PRIMARY KEY NOT NULL,
    user_name VARCHAR,
    product_id INT,
    review_date DATE,
    review_text VARCHAR,
	sentiment_textblob VARCHAR,
	sentiment_nltk VARCHAR
);
CREATE INDEX IF NOT EXISTS idx_review_id ON product_analyzer.fact_reviews (review_id);

INSERT INTO product_analyzer.fact_reviews
    (user_name,product_id,review_date,review_text,sentiment_textblob,sentiment_nltk)
SELECT 
	src_gsm.user_name,
	src_gsm.product_id,
	src_gsm.date,
	src_gsm.review_text,
	src_gsm.sentiment_textblob,
	src_gsm.sentiment_nltk
FROM product_analyzer.stg_gsm_reviews as src_gsm	
UNION
SELECT
	src_reddit.user_name,
	src_reddit.product_id,
	src_reddit.date,
	src_reddit.review_text,
	src_reddit.sentiment_textblob,
	src_reddit.sentiment_nltk
FROM product_analyzer.stg_reddit_reviews as src_reddit
