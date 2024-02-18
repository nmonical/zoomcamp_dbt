{{
    config(
        materialized='table'
    )
}}

with fhv_trip_data as (
    select *,
    'fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),


dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    fhv_trip_data.tripid, 
    fhv_trip_data.dispatching_base_num, 
    fhv_trip_data.Affiliated_base_number,
    fhv_trip_data.pickup_locationid, 
    fhv_trip_data.dropoff_locationid, 
    fhv_trip_data.pickup_datetime, 
    fhv_trip_data.dropoff_datetime,
    fhv_trip_data.SR_Flag, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone  

from fhv_trip_data 
inner join dim_zones as pickup_zone
on fhv_trip_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_trip_data.dropoff_locationid = dropoff_zone.locationid