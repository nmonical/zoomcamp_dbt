{{ config(materialized='view') }}

SELECT
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    Affiliated_base_number,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    SR_Flag,
   
FROM {{ source('staging', 'fhv') }}
where dispatching_base_num is not null and pickup_datetime like '%2019%'
--dbt build --m <model.sql> --var 'is_test_run: false'
#{% if var('is_test_run', default=true) %}

#LIMIT 100

#{% endif %}