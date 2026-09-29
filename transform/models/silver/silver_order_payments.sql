SELECT distinct on (order_id,payment_sequential)
	order_id::varchar(32),
	payment_sequential,
	payment_type::varchar(20),
	payment_installments,
	payment_value::numeric(10,2)
FROM {{source('bronze','stream_order_payments')}}
where order_id is not null and payment_sequential is not null
order by order_id, payment_sequential asc

