with cleaned_quotes as (
    select
        symbol,
        try_cast(current_price as double) as price_value,
        market_timestamp
    from {{ ref('silver_clean_stock_quotes') }}
    where try_cast(current_price as double) is not null
),

max_trade_date as (
    select
        cast(
            to_timestamp_ltz(max(market_timestamp)) as date
        ) as trade_date
    from cleaned_quotes
),

daily_avg_price as (
    select
        cq.symbol,
        avg(cq.price_value) as avg_price
    from cleaned_quotes cq
    where cast(
        to_timestamp_ltz(cq.market_timestamp) as date
    ) = (select trade_date from max_trade_date)
    group by cq.symbol
),

symbol_volatility as (
    select
        symbol,
        stddev_pop(price_value) as volatility,
        stddev_pop(price_value)
            / nullif(avg(price_value), 0) as relative_volatility
    from cleaned_quotes
    group by symbol
)

select
    d.symbol,
    d.avg_price,
    s.volatility,
    s.relative_volatility
from daily_avg_price d
inner join symbol_volatility s
    on d.symbol = s.symbol
order by d.symbol;


