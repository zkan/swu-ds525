with

orders as (

    select * from {{ ref('stg_jaffle__orders') }}

)

, customers as (

    select * from {{ ref('stg_jaffle__customers') }}

)

, final as (

    select
        o.id as order_id
        , o.order_date
        , o.status as order_status
        , c.name as customer_name

    from orders as o
    join customers as c
    on
        o.user_id = c.id

)

select * from final