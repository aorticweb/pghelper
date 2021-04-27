\c postgres;
-- GRANT ALL PRIVILEGES ON DATABASE postgres TO aorticweb;
CREATE TABLE IF NOT EXISTS orders(
 id SERIAL,
 item_name VARCHAR(200),
 price NUMERIC(15, 2) default 0,
 client_name VARCHAR(100),
 identifier VARCHAR(50)
);