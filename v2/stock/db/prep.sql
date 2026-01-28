DROP TABLE IF EXISTS stock_feature_base;

CREATE TEMPORARY TABLE stock_feature_base AS WITH base AS (
    SELECT
        s.stock_profile,
        s.timestamp,
        s.close,
        s.previous,
        s.open_price,
        s.high,
        s.low,
        s.volume,
        s.value,
        s.frequency,
        s.tradeble_shares,
        s.foreign_buy,
        s.foreign_sell,
        s.bid,
        s.offer,
        s.bid_volume,
        s.offer_volume,
        -- returns
        (s.close - s.previous) / NULLIF(s.previous, 0) AS ret_1d,
        (s.close / LAG(s.close, 5) OVER w) - 1 AS ret_5d,
        (s.close / LAG(s.close, 20) OVER w) - 1 AS ret_20d,
        -- price action
        (s.open_price - s.previous) / NULLIF(s.previous, 0) AS gap,
        (s.high - s.low) / NULLIF(s.previous, 0) AS intraday_range,
        (s.close - s.low) / NULLIF(s.high - s.low, 0) AS close_position
    FROM
        stock_data s WINDOW w AS (
            PARTITION BY s.stock_profile
            ORDER BY
                s.timestamp
        )
)
SELECT
    *
FROM
    base;

DROP TABLE IF EXISTS stock_feature_vol;

CREATE TEMPORARY TABLE stock_feature_vol AS
SELECT
    *,
    STDDEV_SAMP(ret_1d) OVER w5 AS vol_5,
    STDDEV_SAMP(ret_1d) OVER w20 AS vol_20,
    STDDEV_SAMP(ret_1d) OVER w60 AS vol_60,
    close / MAX(close) OVER w120 - 1 AS drawdown
FROM
    stock_feature_base WINDOW w5 AS (
        PARTITION BY stock_profile
        ORDER BY
            timestamp ROWS BETWEEN 4 PRECEDING
            AND CURRENT ROW
    ),
    w20 AS (
        PARTITION BY stock_profile
        ORDER BY
            timestamp ROWS BETWEEN 19 PRECEDING
            AND CURRENT ROW
    ),
    w60 AS (
        PARTITION BY stock_profile
        ORDER BY
            timestamp ROWS BETWEEN 59 PRECEDING
            AND CURRENT ROW
    ),
    w120 AS (
        PARTITION BY stock_profile
        ORDER BY
            timestamp ROWS BETWEEN 119 PRECEDING
            AND CURRENT ROW
    );

DROP TABLE IF EXISTS stock_feature_liquidity;

CREATE TEMPORARY TABLE stock_feature_liquidity AS
SELECT
    *,
    value AS dollar_volume,
    volume / NULLIF(tradeble_shares, 0) AS turnover,
    (volume - AVG(volume) OVER w20) / NULLIF(STDDEV_SAMP(volume) OVER w20, 0) AS volume_z,
    (foreign_buy - foreign_sell) / NULLIF(volume, 0) AS foreign_flow,
    (bid_volume - offer_volume) / NULLIF(bid_volume + offer_volume, 0) AS order_imbalance,
    (offer - bid) / NULLIF(close, 0) AS spread_proxy
FROM
    stock_feature_vol WINDOW w20 AS (
        PARTITION BY stock_profile
        ORDER BY
            timestamp ROWS BETWEEN 19 PRECEDING
            AND CURRENT ROW
    );

DROP TABLE IF EXISTS stock_feature_market;

CREATE TEMPORARY TABLE stock_feature_market AS WITH joined AS (
    SELECT
        s.*,
        i.close AS index_close,
        (
            i.close - LAG(i.close, 1) OVER (
                ORDER BY
                    i.timestamp
            )
        ) / NULLIF(
            LAG(i.close, 1) OVER (
                ORDER BY
                    i.timestamp
            ),
            0
        ) AS index_ret_1d
    FROM
        stock_feature_liquidity s
        JOIN index_data i ON s.timestamp = i.timestamp
)
SELECT
    *,
    ret_5d - (
        index_close / LAG(index_close, 5) OVER (
            ORDER BY
                timestamp
        ) - 1
    ) AS rel_ret_5,
    STDDEV_SAMP(ret_1d) OVER w20 / NULLIF(STDDEV_SAMP(index_ret_1d) OVER w20, 0) AS rel_vol,
    CORR(ret_1d, index_ret_1d) OVER w20 AS corr_20
FROM
    joined WINDOW w20 AS (
        PARTITION BY stock_profile
        ORDER BY
            timestamp ROWS BETWEEN 19 PRECEDING
            AND CURRENT ROW
    );

DROP TABLE IF EXISTS stock_feature_calendar;

CREATE TEMPORARY TABLE stock_feature_calendar AS
SELECT
    *,
    SIN(2 * PI() * DAYOFWEEK(timestamp) / 7) AS dow_sin,
    COS(2 * PI() * DAYOFWEEK(timestamp) / 7) AS dow_cos,
    SIN(2 * PI() * WEEKOFYEAR(timestamp) / 52) AS woy_sin,
    COS(2 * PI() * WEEKOFYEAR(timestamp) / 52) AS woy_cos,
    SIN(2 * PI() * MONTH(timestamp) / 12) AS month_sin,
    COS(2 * PI() * MONTH(timestamp) / 12) AS month_cos
FROM
    stock_feature_market;

