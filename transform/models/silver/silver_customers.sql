SELECT
	customer_id::varchar(32),
	customer_unique_id::varchar(32),
	customer_zip_code_prefix::decimal(5,0),
	initcap(customer_city)::varchar(50) as customer_city,
	upper(customer_state)::varchar(2) as customer_state

FROM {{source('bronze','pg_customers')}}
where customer_id is not null
