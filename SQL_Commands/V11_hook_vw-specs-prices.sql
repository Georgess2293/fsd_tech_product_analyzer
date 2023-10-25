CREATE OR REPLACE VIEW product_analyzer.specs_prices AS(
SELECT
	prod.product_id,
	prod.brand,
	prod.model,
	specs.display_type,
	specs.display_size,
	specs.platform_os,
	specs.platform_chipset,
	specs.memory_internal,
	specs.main_camera,
    SUBSTRING(specs.main_camera, '([0-9.]+ MP)') AS camera,
	specs.sound_loudspeaker,
	 CASE
        WHEN POSITION(' ' IN specs.battery_type) > 0
        THEN substring(specs.battery_type FROM POSITION(' ' IN specs.battery_type) + 1)
        ELSE specs.battery_type
    END AS battery_type,
	prices.avg_price
FROM product_analyzer.dim_product AS prod
INNER JOIN product_analyzer.dim_specs AS specs
ON prod.product_id=specs.product_id
INNER JOIN product_analyzer.agg_prices AS prices
ON specs.product_id=prices.product_id
	)