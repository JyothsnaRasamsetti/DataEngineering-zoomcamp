select
  -- identifiers
  vendorid,
  ratecodeid,
  pickup_locationid,
  dropoff_locationid,
  service_type,

  -- timestamps
  pickup_datetime,
  dropoff_datetime,

  -- trip info
  store_and_fwd_flag,
  passenger_count,
  trip_distance,
  trip_type,

  -- payment info
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  ehail_fee,
  improvement_surcharge,
  total_amount,
  payment_type

from {{ ref('int_trips_unioned') }}