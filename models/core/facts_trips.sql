{{
    config(
        materialized='table'
    )
}}

-- encadeamento de CTE's
/*
    1-> 2 CTE's para referenciar as 2 tabelas de staging (yellow e green), incluindo flags para diferenciar os 2 serviços facilmente
    2-> união dops resultados das 2 CTE's anteriores em uma única CTE
    3-> CTE para a tabela de referência (criada baseada em seed)
    4-> JOIN's entre a CTE que faz a união e a tabela de referência (referenciada 2 vezes)  
*/

with green_tripdata as (
    select *, 
        'Green' as service_type -- flag para diferenciar corridas de taxis verdes
    from {{ ref('stg_green_tripdata') }}
), 
yellow_tripdata as (
    select *, 
        'Yellow' as service_type -- flag para diferenciar corridas de taxis amarelos
    from {{ ref('stg_yellow_tripdata') }}
), 
trips_unioned as (
    select * from green_tripdata -- aqui é preciso garantir que ambos os modelos tenham a MESMA ESTRUTURA
    union all 
    select * from yellow_tripdata
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown' -- retirando da consulta linhas cuja coluna 'borough' é Unknown
)
select trips_unioned.tripid, 
    trips_unioned.vendorid, 
    trips_unioned.service_type,
    trips_unioned.ratecodeid, 
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_type_description
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid