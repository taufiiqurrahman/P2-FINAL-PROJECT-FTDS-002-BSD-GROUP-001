-- Table for customer signups (channels and reasons)
CREATE TABLE customer_cases(
	"index" INT,
	"case_id" VARCHAR(20),
	"date_time" VARCHAR(50),
	"customer_id" VARCHAR(50),
	"channel" VARCHAR(20),
	"reason" VARCHAR(20)
);

-- Table for customer demographic
CREATE TABLE customer_info(
	"index" INT,
	"customer_id" VARCHAR(50),
	"age" INT,
	"gender" VARCHAR(10)
);

-- Table for customer signup and cancellation date
CREATE TABLE customer_product(
	"index" INT,
	"customer_id" VARCHAR(50),
	"product" VARCHAR(10),
	"signup_date_time" VARCHAR(50),
	"cancel_date_time" VARCHAR(50)
);

-- Table for product details
CREATE TABLE product_info(
	"product_id" VARCHAR(10),
	"name" VARCHAR(50),
	"price" INT,
	"billing_cycle" INT
);

SELECT * FROM customer_cases;
SELECT * FROM customer_info;
SELECT * FROM customer_product;
SELECT * FROM product_info;