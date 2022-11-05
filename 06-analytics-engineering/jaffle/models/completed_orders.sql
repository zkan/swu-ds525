select
    o.id
    , o.order_date
    , o.status
    , c.first_name
    , c.last_name

from {{ source('jaffle', 'jaffle_shop_orders') }} as o
join {{ source('jaffle', 'jaffle_shop_customers') }} as c
on
    o.user_id = c.id
where status = 'completed'