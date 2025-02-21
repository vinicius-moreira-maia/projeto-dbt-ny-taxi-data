-- dados serão gravados no banco como views
{{
    config(
        materialized='view'
    )
}}

-- definindo a CTE, para ser usada na consulta de criação do modelo
-- sleciona todas as colunas originais
-- usa os mesmos 2 campos usados para criar a SK, para criar um identioficador único e filtrar as duplicatas
with tripdata as 
(
  select *,
    row_number() over(partition by vendorid, lpep_pickup_datetime) as rn
  from {{ source('staging','green_tripdata') }} -- dados originais da tabela de estaging
  where vendorid is not null 
)
select
    -- aqui é basicamente seleção e transformação de colunas

    -- criando sk e convertendo tipos de forma segura
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid,
    {{ dbt.safe_cast("vendorid", api.Column.translate_type("integer")) }} as vendorid,
    {{ dbt.safe_cast("ratecodeid", api.Column.translate_type("integer")) }} as ratecodeid,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- colunas de timestamp
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- informações da corrida
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    {{ dbt.safe_cast("trip_type", api.Column.translate_type("integer")) }} as trip_type,

    -- informações de pagamento
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},0) as payment_type,
    {{ get_payment_type_description("payment_type") }} as payment_type_description

from tripdata -- tabela definida na CTE
where rn = 1 -- seleciona apenas a primeira ocorrência de cada vendedor (remove duplicatas)


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
-- limitando o teste a 100 registros, se a variável 'is_test_run' for true (valor default)
-- com isso eu evito carregar grandes volumes de dados necessariamente
-- MUITO ÚTIL
{% if var('is_test_run', default=false) %} -- o default é bom ser true

  limit 100

{% endif %} 