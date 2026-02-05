SET
    @WINDOW := 60;

SET
    @HORIZON := 5;

SET
    @VAL_RATIO := 0.2;

SET
    @MIN_STEP := 520;

DELETE FROM
    stock_profiles
WHERE
    stock_code IN (
        SELECT
            stock_profile
        FROM
            stock_data
        GROUP BY
            stock_profile
        HAVING
            MAX(timestamp) != (
                SELECT
                    MAX(timestamp)
                FROM
                    stock_data
            )
    );

DROP TABLE IF EXISTS stock_return_base;

CREATE TEMPORARY TABLE stock_return_base AS WITH base AS (
    SELECT
        s.stock_profile,
        s.timestamp,
        s.close,
        s.previous,
        CASE
            WHEN s.open_price = 0
            AND s.previous != 0 THEN s.previous
            ELSE s.open_price
        END AS open_price,
        s.high,
        s.low,
        s.volume,
        s.value,
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
        (
            CASE
                WHEN s.open_price = 0
                AND s.previous != 0 THEN s.previous
                ELSE s.open_price
            END - s.previous
        ) / NULLIF(s.previous, 0) AS gap,
        (s.high - s.low) / NULLIF(s.previous, 0) AS intraday_range,
        CASE
            WHEN s.high != s.low THEN (s.close - s.low) / NULLIF(s.high - s.low, 0)
            ELSE 0.5
        END AS close_position,
        ROW_NUMBER() OVER (
            PARTITION BY s.stock_profile
            ORDER BY
                s.timestamp DESC
        ) AS step_count,
        COUNT(*) OVER (PARTITION BY s.stock_profile) as total_step,
        -- future return target
        (LEAD(s.close, @HORIZON) OVER w / s.close) - 1 AS future_return_5d
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
    base
WHERE
    future_return_5d IS NOT NULL
    AND total_step >= @MIN_STEP
    AND step_count < (total_step -120);

DROP TABLE IF EXISTS stock_return_features;

CREATE TEMPORARY TABLE stock_return_features AS
SELECT
    *,
    STDDEV_SAMP(ret_1d) OVER w20 AS vol_20,
    close / MAX(close) OVER w120 - 1 AS drawdown,
    volume / NULLIF(tradeble_shares, 0) AS turnover,
    (foreign_buy - foreign_sell) / NULLIF(volume, 0) AS foreign_flow,
    (bid_volume - offer_volume) / NULLIF(bid_volume + offer_volume, 0) AS order_imbalance,
    (offer - bid) / NULLIF(close, 0) AS spread_proxy
FROM
    stock_return_base WINDOW w20 AS (
        PARTITION BY stock_profile
        ORDER BY
            timestamp ROWS BETWEEN 19 PRECEDING
            AND CURRENT ROW
    ),
    w120 AS (
        PARTITION BY stock_profile
        ORDER BY
            timestamp ROWS BETWEEN 119 PRECEDING
            AND CURRENT ROW
    );

DROP TABLE IF EXISTS stock_vol_target;

CREATE TEMPORARY TABLE stock_vol_target AS WITH base AS (
    SELECT
        stock_profile,
        timestamp,
        STDDEV_SAMP(ret_1d) OVER (
            PARTITION BY stock_profile
            ORDER BY
                timestamp ROWS BETWEEN 1 FOLLOWING
                AND 20 FOLLOWING
        ) AS future_vol_20d
    FROM
        stock_return_base
)
SELECT
    *
FROM
    base
WHERE
    future_vol_20d IS NOT NULL;

DROP TABLE IF EXISTS stock_drawdown_target;

CREATE TEMPORARY TABLE stock_drawdown_target AS WITH base AS (
    SELECT
        b.stock_profile,
        b.timestamp,
        MIN(f.close / b.close - 1) AS future_drawdown_20d
    FROM
        stock_return_base b
        JOIN stock_data f ON f.stock_profile = b.stock_profile
        AND f.timestamp > b.timestamp
        AND f.timestamp <= DATE_ADD(b.timestamp, INTERVAL 20 DAY)
    GROUP BY
        b.stock_profile,
        b.timestamp
)
SELECT
    *
FROM
    base
WHERE
    future_drawdown_20d IS NOT NULL;

DROP TABLE IF EXISTS stock_crash_target;

CREATE TEMPORARY TABLE stock_crash_target AS WITH base AS (
    SELECT
        stock_profile,
        timestamp,
        CASE
            WHEN future_return_5d < -0.08 THEN 1
            ELSE 0
        END AS crash
    FROM
        stock_return_base
)
SELECT
    *
FROM
    base
WHERE
    crash IS NOT NULL;

DROP TABLE IF EXISTS stock_return_features_cal;

CREATE TEMPORARY TABLE stock_return_features_cal AS
SELECT
    *,
    SIN(2 * PI() * DAYOFWEEK(timestamp) / 7) AS dow_sin,
    COS(2 * PI() * DAYOFWEEK(timestamp) / 7) AS dow_cos,
    SIN(2 * PI() * WEEKOFYEAR(timestamp) / 52) AS woy_sin,
    COS(2 * PI() * WEEKOFYEAR(timestamp) / 52) AS woy_cos,
    SIN(2 * PI() * MONTH(timestamp) / 12) AS month_sin,
    COS(2 * PI() * MONTH(timestamp) / 12) AS month_cos
FROM
    stock_return_features;

DROP TABLE IF EXISTS stock_data_normalized;

CREATE TEMPORARY TABLE stock_data_normalized AS
SELECT
    stock_profile,
    timestamp,
    -- normalized inputs
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY
            ret_1d
    ) AS ret_1d_n,
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY
            ret_5d
    ) AS ret_5d_n,
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY
            ret_20d
    ) AS ret_20d_n,
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY
            gap
    ) AS gap_n,
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY
            intraday_range
    ) AS intraday_range_n,
    close_position,
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY
            vol_20
    ) AS vol_20_n,
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY
            drawdown
    ) AS drawdown_n,
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY
            turnover
    ) AS turnover_n,
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY
            foreign_flow
    ) AS foreign_flow_n,
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY
            order_imbalance
    ) AS order_imbalance_n,
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
        ORDER BY
            spread_proxy
    ) AS spread_n,
    dow_sin,
    dow_cos,
    woy_sin,
    woy_cos,
    month_sin,
    month_cos,
    future_return_5d
FROM
    stock_return_features_cal;

DROP TABLE IF EXISTS stock_return_normalized;

CREATE TEMPORARY TABLE stock_return_normalized AS
SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY stock_profile
        ORDER BY
            timestamp DESC
    ) AS step_count,
    COUNT(*) OVER (PARTITION BY stock_profile) as total_step
FROM
    stock_data_normalized;

DROP TABLE IF EXISTS stock_vol_normalized;

CREATE TEMPORARY TABLE stock_vol_normalized AS
SELECT
    n.stock_profile,
    n.timestamp,
    n.ret_1d_n,
    n.ret_5d_n,
    n.ret_20d_n,
    n.vol_20_n,
    n.drawdown_n,
    n.turnover_n,
    n.foreign_flow_n,
    n.order_imbalance_n,
    n.spread_n,
    ROW_NUMBER() OVER (
        PARTITION BY n.stock_profile
        ORDER BY
            n.timestamp DESC
    ) AS step_count,
    COUNT(*) OVER (PARTITION BY n.stock_profile) as total_step,
    v.future_vol_20d
FROM
    stock_data_normalized n
    JOIN stock_vol_target v ON n.stock_profile = v.stock_profile
    AND n.timestamp = v.timestamp;

DROP TABLE IF EXISTS stock_drawdown_normalized;

CREATE TEMPORARY TABLE stock_drawdown_normalized AS
SELECT
    n.stock_profile,
    n.timestamp,
    n.ret_1d_n,
    n.ret_5d_n,
    n.vol_20_n,
    n.drawdown_n,
    n.turnover_n,
    n.foreign_flow_n,
    n.order_imbalance_n,
    n.spread_n,
    ROW_NUMBER() OVER (
        PARTITION BY n.stock_profile
        ORDER BY
            n.timestamp DESC
    ) AS step_count,
    COUNT(*) OVER (PARTITION BY n.stock_profile) as total_step,
    d.future_drawdown_20d
FROM
    stock_data_normalized n
    JOIN stock_drawdown_target d ON n.stock_profile = d.stock_profile
    AND n.timestamp = d.timestamp;

DROP TABLE IF EXISTS stock_crash_normalized;

CREATE TEMPORARY TABLE stock_crash_normalized AS
SELECT
    n.stock_profile,
    n.timestamp,
    -- USE RAW (NOT SEQUENCE) FEATURES
    n.vol_20,
    n.drawdown,
    n.turnover,
    n.foreign_flow,
    n.order_imbalance,
    n.spread_proxy,
    ROW_NUMBER() OVER (
        PARTITION BY n.stock_profile
        ORDER BY
            n.timestamp DESC
    ) AS step_count,
    COUNT(*) OVER (PARTITION BY n.stock_profile) as total_step,
    c.crash
FROM
    stock_return_features n
    JOIN stock_crash_target c ON n.stock_profile = c.stock_profile
    AND n.timestamp = c.timestamp;

DROP TABLE IF EXISTS stock_return_train;

DROP TABLE IF EXISTS stock_return_val;

CREATE TABLE stock_return_train AS
SELECT
    stock_profile,
    timestamp,
    -- normalized inputs
    ret_1d_n,
    ret_5d_n,
    ret_20d_n,
    gap_n,
    intraday_range_n,
    close_position,
    vol_20_n,
    drawdown_n,
    turnover_n,
    foreign_flow_n,
    order_imbalance_n,
    spread_n,
    dow_sin,
    dow_cos,
    woy_sin,
    woy_cos,
    month_sin,
    month_cos,
    future_return_5d
FROM
    stock_return_normalized
WHERE
    step_count > ROUND(total_step * @VAL_RATIO, 0);

CREATE TABLE stock_return_val AS
SELECT
    stock_profile,
    timestamp,
    -- normalized inputs
    ret_1d_n,
    ret_5d_n,
    ret_20d_n,
    gap_n,
    intraday_range_n,
    close_position,
    vol_20_n,
    drawdown_n,
    turnover_n,
    foreign_flow_n,
    order_imbalance_n,
    spread_n,
    dow_sin,
    dow_cos,
    woy_sin,
    woy_cos,
    month_sin,
    month_cos,
    future_return_5d
FROM
    stock_return_normalized
WHERE
    step_count <= ROUND(total_step * @VAL_RATIO, 0);

DROP TABLE IF EXISTS stock_vol_train;

DROP TABLE IF EXISTS stock_vol_val;

CREATE TABLE stock_vol_train AS
SELECT
    stock_profile,
    timestamp,
    ret_1d_n,
    ret_5d_n,
    ret_20d_n,
    vol_20_n,
    drawdown_n,
    turnover_n,
    foreign_flow_n,
    order_imbalance_n,
    spread_n,
    future_vol_20d
FROM
    stock_vol_normalized
WHERE
    step_count > ROUND(total_step * @VAL_RATIO, 0);

CREATE TABLE stock_vol_val AS
SELECT
    stock_profile,
    timestamp,
    ret_1d_n,
    ret_5d_n,
    ret_20d_n,
    vol_20_n,
    drawdown_n,
    turnover_n,
    foreign_flow_n,
    order_imbalance_n,
    spread_n,
    future_vol_20d
FROM
    stock_vol_normalized
WHERE
    step_count <= ROUND(total_step * @VAL_RATIO, 0);

DROP TABLE IF EXISTS stock_drawdown_train;

DROP TABLE IF EXISTS stock_drawdown_val;

CREATE TABLE stock_drawdown_train AS
SELECT
    stock_profile,
    timestamp,
    ret_1d_n,
    ret_5d_n,
    vol_20_n,
    drawdown_n,
    turnover_n,
    foreign_flow_n,
    order_imbalance_n,
    spread_n,
    future_drawdown_20d
FROM
    stock_drawdown_normalized
WHERE
    step_count > ROUND(total_step * @VAL_RATIO, 0);

CREATE TABLE stock_drawdown_val AS
SELECT
    stock_profile,
    timestamp,
    ret_1d_n,
    ret_5d_n,
    vol_20_n,
    drawdown_n,
    turnover_n,
    foreign_flow_n,
    order_imbalance_n,
    spread_n,
    future_drawdown_20d
FROM
    stock_drawdown_normalized
WHERE
    step_count <= ROUND(total_step * @VAL_RATIO, 0);

DROP TABLE IF EXISTS stock_crash_train;

DROP TABLE IF EXISTS stock_crash_val;

CREATE TABLE stock_crash_train AS
SELECT
    stock_profile,
    timestamp,
    vol_20,
    drawdown,
    turnover,
    foreign_flow,
    order_imbalance,
    spread_proxy,
    crash
FROM
    stock_crash_normalized
WHERE
    step_count > ROUND(total_step * @VAL_RATIO, 0);

CREATE TABLE stock_crash_val AS
SELECT
    stock_profile,
    timestamp,
    vol_20,
    drawdown,
    turnover,
    foreign_flow,
    order_imbalance,
    spread_proxy,
    crash
FROM
    stock_crash_normalized
WHERE
    step_count <= ROUND(total_step * @VAL_RATIO, 0);