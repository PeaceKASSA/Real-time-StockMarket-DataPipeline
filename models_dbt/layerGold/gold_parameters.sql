with ranked as (
    select
        *,
        row_number() over (
            partition by symbol
            order by fetched_at desc
        ) as rn
    from {{ ref('silver_clean_stock_quotes') }}
)

select
    symbol,
    current_price,
    change_amount,
    change_percent
from ranked
where rn = 1
