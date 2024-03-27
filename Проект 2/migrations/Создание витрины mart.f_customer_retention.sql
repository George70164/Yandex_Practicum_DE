DROP table if exists mart.f_customer_retention;

CREATE TABLE mart.f_customer_retention (
	item_id int4 NOT NULL,
	new_customers_count int4 NULL,
	returning_customers_count int4 NULL,
	refunded_customer_count int4 NULL,
	period_name varchar(20) NOT NULL default 'weekly'::character varying,
	period_id int4 NOT NULL,
	new_customers_revenue numeric(10, 2) null,
	returning_customers_revenue numeric(10, 2) null,
	customers_refunded int4 null);