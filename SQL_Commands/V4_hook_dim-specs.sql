CREATE TABLE IF NOT EXISTS product_analyzer.dim_specs
(
    specs_id SERIAL PRIMARY KEY NOT NULL,
    product_id INT UNIQUE,
	display_type VARCHAR,
	display_size VARCHAR,
	platform_os VARCHAR,
    platform_chipset VARCHAR,
	memory_internal VARCHAR,
	main_camera VARCHAR,
	sound_loudspeaker VARCHAR,
	battery_type VARCHAR
);
CREATE INDEX IF NOT EXISTS idx_specs_id ON product_analyzer.dim_specs (specs_id);

INSERT INTO product_analyzer.dim_specs
    (product_id,display_type,display_size,platform_os,platform_chipset,memory_internal,main_camera,sound_loudspeaker,battery_type)
SELECT
	src_specs.product_id,
	 CASE
		WHEN POSITION(',' IN src_specs.display_type) > 0
		THEN SUBSTRING(src_specs.display_type FROM 1 FOR POSITION(',' IN src_specs.display_type) - 1)
		ELSE src_specs.display_type
    END AS display_type,
	SUBSTRING(src_specs.display_size, '([0-9.]+ inches)') AS display_size,
	src_specs.platform_os,
	 CASE
		WHEN POSITION('-' IN src_specs.platform_chipset) > 0
		THEN SUBSTRING(src_specs.platform_chipset FROM 1 FOR POSITION('-' IN src_specs.platform_chipset) - 1)
		ELSE src_specs.platform_chipset
	END AS platform_chipset,
	src_specs.memory_internal,
	--SUBSTRING(src_specs.main_camera_triple, '([0-9.]+ MP)') AS main_camera,
	src_specs.main_camera_triple,
	src_specs.sound_loudspeaker,
	 CASE
		WHEN POSITION(',' IN src_specs.battery_type) > 0
		THEN SUBSTRING(src_specs.battery_type FROM 1 FOR POSITION(',' IN src_specs.battery_type) - 1)
		ELSE src_specs.battery_type
	END AS battery_type
FROM product_analyzer.stg_products_specs AS src_specs
ON CONFLICT (product_id)
DO UPDATE SET
product_id=EXCLUDED.product_id,
display_type=EXCLUDED.display_type,
display_size=EXCLUDED.display_size,
platform_os=EXCLUDED.platform_os,
platform_chipset=EXCLUDED.platform_chipset,
memory_internal=EXCLUDED.memory_internal,
main_camera=EXCLUDED.main_camera,
sound_loudspeaker=EXCLUDED.sound_loudspeaker,
battery_type=EXCLUDED.battery_type
