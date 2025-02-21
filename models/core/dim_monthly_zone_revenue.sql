{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('facts_trips') }}
)
    select 
    -- as 3 colunas do agrupamento (agrupamento será mensal)
    -- esse macro 'date_trunc', por baixo dos panos, se adapta à sintaxe da base de dados que está sendo utilizada (BQ, no caso)
    pickup_zone as revenue_zone,
    {{ dbt.date_trunc("month", "pickup_datetime") }} as revenue_month, 
    service_type, 

    -- somatórios das receitas 
    sum(fare_amount) as revenue_monthly_fare,
    sum(extra) as revenue_monthly_extra,
    sum(mta_tax) as revenue_monthly_mta_tax,
    sum(tip_amount) as revenue_monthly_tip_amount,
    sum(tolls_amount) as revenue_monthly_tolls_amount,
    sum(ehail_fee) as revenue_monthly_ehail_fee,
    sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    sum(total_amount) as revenue_monthly_total_amount,

    -- cálculos adicionais
    count(tripid) as total_monthly_trips, -- nº mensal de corridas
    avg(passenger_count) as avg_monthly_passenger_count, -- média mensal de passageiros
    avg(trip_distance) as avg_monthly_trip_distance -- média mensal da distância das corridas

    from trips_data
    group by 1,2,3 -- esse agrupamento se referece às 3 primeiras colunas do select