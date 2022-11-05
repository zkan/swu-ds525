select
    status
from {{ ref('completed_orders') }}
where status != 'completed'