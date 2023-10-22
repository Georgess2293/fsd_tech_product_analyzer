CREATE VIEW product_analyzer.avg_prices AS(
    SELECT
        product.product_id,
        product.brand,
        product.model,
        agg_prices.avg_price
    FROM product_analyzer.dim_product AS product
    INNER JOIN product_analyzer.agg_prices AS agg_prices
    ON product.product_id=agg_prices.product_id
)