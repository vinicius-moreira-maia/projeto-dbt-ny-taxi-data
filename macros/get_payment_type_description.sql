{#
Bloco de comentário
#}

-- esse macro retorna a descrição do tipo de pagamento de uma corrida
-- recebe o parâmetro e converte seu tipo, para que seja usado no CASE
-- dessa forma, a coluna é passada para essa função, onde pode haver tratamento de dados

{% macro get_payment_type_description(payment_type) -%}

    case {{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }}  
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'EMPTY'
    end

{%- endmacro %}