USE idx;

DROP TABLE IF EXISTS stock_daily_base;

CREATE TABLE stock_daily_base AS WITH last_timestamps as (
    SELECT
        DISTINCT timestamp
    FROM
        stock_data
    ORDER BY
        timestamp DESC
    LIMIT
        270
)
SELECT
    stock_profile,
    DATE(timestamp) AS date,
    close,
    CASE
        WHEN open_price = 0
        AND previous != 0 THEN previous
        ELSE open_price
    END AS open_price,
    high,
    low,
    volume,
    bid_volume,
    offer_volume,
    foreign_buy,
    foreign_sell,
    non_regular_volume
FROM
    stock_data
WHERE
    timestamp >= (
        SELECT
            MIN(timestamp)
        from
            last_timestamps
    );

DROP TABLE IF EXISTS stock_returns;

CREATE TABLE stock_returns AS
SELECT
    *,
    close / LAG(close, 1) OVER w - 1 AS ret_1d,
    close / LAG(close, 3) OVER w - 1 AS ret_3d,
    close / LAG(close, 5) OVER w - 1 AS ret_5d,
    (high - low) / NULLIF(close, 0) AS hl_range,
    (open_price / LAG(close, 1) OVER w - 1) AS gap_open
FROM
    stock_daily_base WINDOW w AS (
        PARTITION BY stock_profile
        ORDER BY
            date
    );

DROP TABLE IF EXISTS stock_rolling_features;

CREATE TABLE stock_rolling_features AS
SELECT
    *,
    STDDEV(ret_1d) OVER (
        PARTITION BY stock_profile
        ORDER BY
            date ROWS BETWEEN 4 PRECEDING
            AND CURRENT ROW
    ) AS volatility_5d,
    STDDEV(ret_1d) OVER (
        PARTITION BY stock_profile
        ORDER BY
            date ROWS BETWEEN 9 PRECEDING
            AND CURRENT ROW
    ) AS volatility_10d,
    LOG(
        NULLIF(
            volume / NULLIF(
                AVG(volume) OVER (
                    PARTITION BY stock_profile
                    ORDER BY
                        date ROWS BETWEEN 9 PRECEDING
                        AND CURRENT ROW
                ),
                0
            ),
            0
        )
    ) AS volume_ratio_10d
FROM
    stock_returns;

DROP TABLE IF EXISTS stock_flow_features;

CREATE TABLE stock_flow_features AS
SELECT
    *,
    (bid_volume - offer_volume) / NULLIF(bid_volume + offer_volume, 0) AS bid_offer_imbalance,
    (foreign_buy - foreign_sell) / NULLIF(volume, 0) AS foreign_flow_ratio,
    non_regular_volume / NULLIF(volume, 0) AS non_regular_vol_ratio
FROM
    stock_rolling_features;

DROP TABLE IF EXISTS index_daily_features;

CREATE TABLE index_daily_features AS WITH last_timestamps as (
    SELECT
        DISTINCT timestamp
    FROM
        index_data
    ORDER BY
        timestamp DESC
    LIMIT
        270
)
SELECT
    index_profile,
    DATE(timestamp) AS date,
    close,
    close / LAG(close, 1) OVER w - 1 AS index_ret_1d,
    close / LAG(close, 3) OVER w - 1 AS index_ret_3d
FROM
    index_data
WHERE
    index_profile = 'COMPOSITE'
    AND timestamp >= (
        SELECT
            MIN(timestamp)
        from
            last_timestamps
    ) WINDOW w AS (
        PARTITION BY index_profile
        ORDER BY
            DATE(timestamp)
    );

DROP TABLE IF EXISTS stock_with_index;

CREATE TABLE stock_with_index AS
SELECT
    s.*,
    i.index_ret_1d,
    i.index_ret_3d,
    (s.ret_3d - i.index_ret_3d) AS rel_ret_3d
FROM
    stock_flow_features s
    LEFT JOIN index_daily_features i ON s.date = i.date;

DROP TABLE IF EXISTS stock_beta_features;

CREATE TABLE stock_beta_features AS
SELECT
    *,
    (
        AVG(ret_1d * index_ret_1d) OVER w - AVG(ret_1d) OVER w * AVG(index_ret_1d) OVER w
    ) / NULLIF(
        AVG(index_ret_1d * index_ret_1d) OVER w - AVG(index_ret_1d) OVER w * AVG(index_ret_1d) OVER w,
        0
    ) AS beta_60d
FROM
    stock_with_index WINDOW w AS (
        PARTITION BY stock_profile
        ORDER BY
            date ROWS BETWEEN 59 PRECEDING
            AND CURRENT ROW
    );

DROP TABLE IF EXISTS feature_store_daily;

CREATE TABLE feature_store_daily AS
SELECT
    *,
    LEAD(close, 3) OVER w / NULLIF(LEAD(open_price, 1) OVER w - 1, 0) AS fwd_ret_3d,
    LEAD(close, 5) OVER w / NULLIF(LEAD(open_price, 1) OVER w - 1, 0) AS fwd_ret_5d
FROM
    stock_beta_features WINDOW w AS (
        PARTITION BY stock_profile
        ORDER BY
            date
    );

DELETE FROM
    feature_store_daily
WHERE
    ret_5d IS NULL
    OR volatility_10d IS NULL
    OR beta_60d IS NULL
    OR fwd_ret_3d IS NULL;