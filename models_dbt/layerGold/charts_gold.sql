with enriched as (
    select
        q.symbol,
        cast(q.market_timestamp as date) as trade_date,
        q.day_low,
        q.day_high,
        q.current_price,
        first_value(q.current_price) over (
            partition by q.symbol, cast(q.market_timestamp as date)
            order by q.market_timestamp
            rows between unbounded preceding and unbounded following
        ) as candle_open,
        last_value(q.current_price) over (
            partition by q.symbol, cast(q.market_timestamp as date)
            order by q.market_timestamp
            rows between unbounded preceding and unbounded following
        ) as candle_close
    from {{ ref('silver_clean_stock_quotes') }} q
),

candles as (
    select
        symbol,
        candle_time,
        min(day_low)      as candle_low,
        max(day_high)     as candle_high,
        max(candle_open)  as candle_open,
        max(candle_close) as candle_close,
        avg(current_price) as trend_line
    from (
        select
            symbol,
            trade_date as candle_time,
            day_low,
            day_high,
            current_price,
            candle_open,
            candle_close
        from enriched
    )
    group by symbol, candle_time
),

ranked as (
    select
        *,
        row_number() over (
            partition by symbol
            order by candle_time desc
        ) as rn
    from candles
)

select
    symbol,
    candle_time,
    candle_low,
    candle_high,
    candle_open,
    candle_close,
    trend_line
from ranked
where rn <= 12
order by symbol, candle_time;
