with trips as (
  select
    {{ dbt_utils.generate_surrogate_key([
    'vendorid',
    'pickup_datetime',
    'dropoff_datetime',
    'pickup_locationid',
    'dropoff_locationid',
    'total_amount',
    'service_type'
]) }} as trip_id,

    vendorid,
    ratecodeid,
    pickup_locationid,
    dropoff_locationid,
    service_type,

    pickup_datetime,
    dropoff_datetime,

    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_type,

    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,

    cast(payment_type as int64) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description

  from {{ ref('int_trips_unioned') }}
),

deduped as (
  select *
  from trips
  where fare_amount > 0
  qualify row_number() over (
    partition by trip_id
    order by pickup_datetime, dropoff_datetime
  ) = 1
)

select * from deduped