-- models/cadastro_transformed.sql
with source as (
    select
        id as id,
        data as data_cadastro,
        {{ capitalize_names('nome') }} as nome
    from {{ ref('cadastro2') }}
)

select * from source
