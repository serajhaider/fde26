SELECT
    geolocation_zip_code_prefix::numeric(5,0),
    AVG(geolocation_lat)::double precision AS geolocation_lat,
    AVG(geolocation_lng)::double precision  AS geolocation_lng,
    MODE() WITHIN GROUP (ORDER BY geolocation_city)::varchar(50) AS geolocation_city,
    MODE() WITHIN GROUP (ORDER BY geolocation_state)::varchar(2) AS geolocation_state
FROM {{source('bronze','api_geolocation')}}
GROUP BY geolocation_zip_code_prefix

