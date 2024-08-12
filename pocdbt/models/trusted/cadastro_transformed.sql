-- models/cadastro_transformed.sql
with source as (
    select
        id,
        data,
        {{ capitalize_names('nome') }} as nome
    from {{ ref('cadastro') }}
)

select * from source
