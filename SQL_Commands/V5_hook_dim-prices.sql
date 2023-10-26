CREATE TABLE IF NOT EXISTS product_analyzer.dim_prices
(
    price_id SERIAL PRIMARY KEY NOT NULL,
    product_id INT UNIQUE,
    _128gb_8gb_ram FLOAT,
    _128gb_6gb_ram FLOAT,
    _256gb_6gb_ram FLOAT,
    _128gb_12gb_ram FLOAT,
    _512gb_16gb_ram FLOAT,
    _256gb_12gb_ram FLOAT,
    _32gb_3gb_ram FLOAT,
    _64gb_4gb_ram FLOAT,
    _128gb_4gb_ram FLOAT,
    _512gb_12gb_ram FLOAT,
    _64gb_6gb_ram FLOAT,
    _256gb_6gb_ram FLOAT,
    _512gb_6gb_ram FLOAT,
    _256gb_4gb_ram FLOAT,
    _512gb_4gb_ram FLOAT,
    _512gb_8gb_ram FLOAT

);
CREATE INDEX IF NOT EXISTS idx_price_id ON product_analyzer.dim_prices (price_id);

INSERT INTO product_analyzer.dim_prices
    (product_id,
    _128gb_8gb_ram,
    _128gb_6gb_ram,
    _256gb_8gb_ram,
    _128gb_12gb_ram,
    _512gb_16gb_ram,
    _256gb_12gb_ram,
    _32gb_3gb_ram,
    _64gb_4gb_ram,
    _128gb_4gb_ram,
    _512gb_12gb_ram,
    _64gb_6gb_ram,
    _256gb_6gb_ram,
    _512gb_6gb_ram,
    _256gb_4gb_ram,
    _512gb_4gb_ram,
    _512gb_8gb_ram
      )
SELECT 
	product_id,
    _128gb_8gb_ram,
    _128gb_6gb_ram,
    _256gb_8gb_ram,
    _128gb_12gb_ram,
    _512gb_16gb_ram,
    _256gb_12gb_ram,
    _32gb_3gb_ram,
    _64gb_4gb_ram,
    _128gb_4gb_ram,
    _512gb_12gb_ram,
    _64gb_6gb_ram,
    _256gb_6gb_ram,
    _512gb_6gb_ram,
    _256gb_4gb_ram,
    _512gb_4gb_ram,
    _512gb_8gb_ram
FROM product_analyzer.stg_products_prices1 src_prices
ON CONFLICT (product_id)
DO UPDATE SET
    _128gb_8gb_ram=excluded._128gb_8gb_ram,
    _128gb_6gb_ram=excluded._128gb_6gb_ram,
    _256gb_8gb_ram=excluded._256gb_6gb_ram,
    _128gb_12gb_ram=excluded._128gb_12gb_ram,
    _512gb_16gb_ram=excluded._512gb_16gb_ram,
    _256gb_12gb_ram=excluded._256gb_12gb_ram,
    _32gb_3gb_ram=excluded._32gb_3gb_ram,
    _64gb_4gb_ram=excluded._64gb_4gb_ram,
    _128gb_4gb_ram=excluded._128gb_4gb_ram,
    _512gb_12gb_ram=excluded._512gb_12gb_ram,
    _64gb_6gb_ram=excluded._64gb_6gb_ram,
    _256gb_6gb_ram=excluded._256gb_6gb_ram,
    _512gb_6gb_ram=excluded._512gb_6gb_ram,
    _256gb_4gb_ram=excluded._256gb_4gb_ram,
    _512gb_4gb_ram=excluded._512gb_4gb_ram,
    _512gb_8gb_ram=excluded._512gb_8gb_ram;