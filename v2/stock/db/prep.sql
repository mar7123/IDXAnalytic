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

DROP TABLE IF EXISTS market_base;

CREATE TEMPORARY TABLE market_base AS WITH index_base AS (
    SELECT
        timestamp,
        (close / previous) - 1 AS idx_ret_1d,
        (
            close / LAG(close, 5) OVER (
                ORDER BY
                    timestamp
            )
        ) - 1 AS idx_ret_5d,
        CASE
            WHEN highest = lowest THEN 0.5
            ELSE (close - lowest) / (highest - lowest)
        END AS idx_close_pos,
        (highest - lowest) / previous AS idx_range,
        value AS idx_value,
        ROW_NUMBER() OVER (
            ORDER BY
                timestamp ASC
        ) AS step_count
    FROM
        index_data
    WHERE
        index_profile = "COMPOSITE"
),
index_window_base AS (
    SELECT
        *,
        STDDEV(idx_ret_1d) OVER w idx_vol_20,
        AVG(idx_value) OVER w avg_val_20,
        STDDEV(idx_value) OVER w AS std_val_20
    FROM
        index_base WINDOW w AS (
            ORDER BY
                timestamp ROWS BETWEEN 19 PRECEDING
                AND CURRENT ROW
        )
),
currency_exchange_base AS (
    SELECT
        timestamp,
        (secondary_value / primary_value) AS currency_exchange_rate
    FROM
        currency_exchange_rates
    WHERE
        primary_code = "USD"
        AND secondary_code = "IDR"
),
merged_timestamp_base AS(
    SELECT
        iwb.*,
        -- Currency exchange features
        ceb.currency_exchange_rate,
        LAG(ceb.currency_exchange_rate, 1) OVER (
            ORDER BY
                iwb.timestamp
        ) AS prev_currency_exchange_rate,
        AVG(ceb.currency_exchange_rate) OVER (
            ORDER BY
                iwb.timestamp ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
        ) AS currency_exchange_rate_ma_7,
        AVG(ceb.currency_exchange_rate) OVER (
            ORDER BY
                iwb.timestamp ROWS BETWEEN 29 PRECEDING
                AND CURRENT ROW
        ) AS currency_exchange_rate_ma_30,
        STDDEV(ceb.currency_exchange_rate) OVER (
            ORDER BY
                iwb.timestamp ROWS BETWEEN 6 PRECEDING
                AND CURRENT ROW
        ) AS currency_exchange_rate_volatility_7
    FROM
        index_window_base iwb
        INNER JOIN currency_exchange_base ceb ON iwb.timestamp = ceb.timestamp
)
SELECT
    timestamp,
    idx_ret_1d,
    idx_ret_5d,
    idx_close_pos,
    idx_range,
    idx_vol_20,
    (idx_value - avg_val_20) / NULLIF(std_val_20, 0) AS idx_value_z_n,
    -- Currency exchange features
    (
        (
            currency_exchange_rate - prev_currency_exchange_rate
        ) / prev_currency_exchange_rate
    ) AS currency_exchange_rate_daily_return,
    LN(
        currency_exchange_rate / prev_currency_exchange_rate
    ) AS currency_exchange_rate_log_return,
    currency_exchange_rate_ma_7,
    currency_exchange_rate_ma_30,
    currency_exchange_rate_volatility_7,
    (
        currency_exchange_rate - currency_exchange_rate_ma_30
    ) / currency_exchange_rate_ma_30 AS currency_exchange_rate_dist_from_ma30
FROM
    merged_timestamp_base
WHERE
    idx_vol_20 IS NOT NULL
    AND step_count > 30;

DROP TABLE IF EXISTS stock_base;

CREATE TEMPORARY TABLE stock_base AS WITH base as (
    SELECT
        s.stock_profile,
        s.timestamp,
        s.close,
        -- suspension check
        s.volume,
        -- derived features
        s.volume / NULLIF(s.tradeble_shares, 0) AS turnover,
        (s.foreign_buy - s.foreign_sell) / NULLIF(s.volume, 0) AS foreign_flow,
        (s.bid_volume - s.offer_volume) / NULLIF(s.bid_volume + s.offer_volume, 0) AS order_imbalance,
        (s.offer - s.bid) / NULLIF(close, 0) AS spread_proxy,
        -- returns
        (s.close - s.previous) / NULLIF(s.previous, 0) AS ret_1d,
        (s.close / LAG(s.close, 5) OVER w) - 1 AS ret_5d,
        (s.close / LAG(s.close, 20) OVER w) - 1 AS ret_20d,
        -- time data
        SIN(2 * PI() * DAYOFWEEK(timestamp) / 7) AS dow_sin,
        COS(2 * PI() * DAYOFWEEK(timestamp) / 7) AS dow_cos,
        SIN(2 * PI() * WEEKOFYEAR(timestamp) / 52) AS woy_sin,
        COS(2 * PI() * WEEKOFYEAR(timestamp) / 52) AS woy_cos,
        SIN(2 * PI() * MONTH(timestamp) / 12) AS month_sin,
        COS(2 * PI() * MONTH(timestamp) / 12) AS month_cos,
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
        close / MAX(close) OVER w60 - 1 AS drawdown,
        -- step counter
        ROW_NUMBER() OVER (
            PARTITION BY s.stock_profile
            ORDER BY
                s.timestamp DESC
        ) AS step_count,
        COUNT(*) OVER (PARTITION BY s.stock_profile) as total_step
    FROM
        stock_data s WINDOW w AS (
            PARTITION BY s.stock_profile
            ORDER BY
                s.timestamp
        ),
        w60 AS (
            PARTITION BY stock_profile
            ORDER BY
                timestamp ROWS BETWEEN 59 PRECEDING
                AND CURRENT ROW
        )
),
window_base AS (
    SELECT
        *,
        STDDEV_SAMP(ret_1d) OVER w20 AS vol_20
    FROM
        base WINDOW w20 AS (
            PARTITION BY stock_profile
            ORDER BY
                timestamp ROWS BETWEEN 19 PRECEDING
                AND CURRENT ROW
        )
)
SELECT
    wb.*,
    -- Index Features
    mb.idx_ret_1d,
    mb.idx_ret_5d,
    mb.idx_close_pos,
    mb.idx_range,
    mb.idx_vol_20,
    mb.idx_value_z_n,
    -- Currency Exchange Rate features
    mb.currency_exchange_rate_daily_return,
    mb.currency_exchange_rate_log_return,
    mb.currency_exchange_rate_ma_7,
    mb.currency_exchange_rate_ma_30,
    mb.currency_exchange_rate_volatility_7,
    mb.currency_exchange_rate_dist_from_ma30
FROM
    window_base wb
    INNER JOIN market_base mb ON wb.timestamp = mb.timestamp
WHERE
    wb.total_step >= @MIN_STEP
    AND wb.step_count < (wb.total_step -60);

DROP TABLE IF EXISTS stock_return_target;

CREATE TEMPORARY TABLE stock_return_target AS WITH base AS (
    SELECT
        stock_profile,
        timestamp,
        -- suspension check
        (LEAD(volume, @HORIZON) OVER w) AS future_volume_5d,
        -- future return target
        (LEAD(close, @HORIZON) OVER w / close) - 1 AS future_return_5d
    FROM
        stock_base WINDOW w AS (
            PARTITION BY stock_profile
            ORDER BY
                timestamp
        )
)
SELECT
    *
FROM
    base
WHERE
    future_return_5d IS NOT NULL;

DROP TABLE IF EXISTS stock_vol_target;

CREATE TEMPORARY TABLE stock_vol_target AS WITH base AS (
    SELECT
        stock_profile,
        timestamp,
        -- suspension check
        (MIN(volume) OVER w) AS min_future_volume_20d,
        -- target
        STDDEV_SAMP(ret_1d) OVER w AS future_vol_20d
    FROM
        stock_base WINDOW w AS (
            PARTITION BY stock_profile
            ORDER BY
                timestamp ROWS BETWEEN 1 FOLLOWING
                AND 20 FOLLOWING
        )
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
        stock_base b
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
        -- suspension check
        (LEAD(volume, @HORIZON) OVER w) AS future_volume_5d,
        -- future return target
        (LEAD(close, @HORIZON) OVER w / close) - 1 AS future_return_5d
    FROM
        stock_base WINDOW w AS (
            PARTITION BY stock_profile
            ORDER BY
                timestamp
        )
)
SELECT
    *,
    CASE
        WHEN future_return_5d < -0.08 THEN 1
        ELSE 0
    END AS crash
FROM
    base
WHERE
    future_return_5d IS NOT NULL;

DROP TABLE IF EXISTS stock_data_normalized;

CREATE TEMPORARY TABLE stock_data_normalized AS WITH base AS (
    SELECT
        stock_profile,
        timestamp,
        -- suspension check
        SUM(volume = 0) OVER (
            PARTITION BY stock_profile
            ORDER BY
                timestamp ROWS BETWEEN 60 PRECEDING
                AND CURRENT ROW
        ) AS zero_volume_count,
        -- stock features
        (ret_1d - AVG(ret_1d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(ret_1d) OVER w, 0),
            0.00000001
        ) AS ret_1d_n,
        (ret_5d - AVG(ret_5d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(ret_5d) OVER w, 0),
            0.00000001
        ) AS ret_5d_n,
        (ret_20d - AVG(ret_20d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(ret_20d) OVER w, 0),
            0.00000001
        ) AS ret_20d_n,
        (gap - AVG(gap) OVER w) / COALESCE(NULLIF(STDDEV_SAMP(gap) OVER w, 0), 0.00000001) AS gap_n,
        (intraday_range - AVG(intraday_range) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(intraday_range) OVER w, 0),
            0.00000001
        ) AS intraday_range_n,
        close_position,
        (vol_20 - AVG(vol_20) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(vol_20) OVER w, 0),
            0.00000001
        ) AS vol_20_n,
        (drawdown - AVG(drawdown) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(drawdown) OVER w, 0),
            0.00000001
        ) AS drawdown_n,
        (turnover - AVG(turnover) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(turnover) OVER w, 0),
            0.00000001
        ) AS turnover_n,
        (foreign_flow - AVG(foreign_flow) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(foreign_flow) OVER w, 0),
            0.00000001
        ) AS foreign_flow_n,
        (order_imbalance - AVG(order_imbalance) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(order_imbalance) OVER w, 0),
            0.00000001
        ) AS order_imbalance_n,
        (spread_proxy - AVG(spread_proxy) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(spread_proxy) OVER w, 0),
            0.00000001
        ) AS spread_proxy_n,
        -- Index features
        (idx_ret_1d - AVG(idx_ret_1d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(idx_ret_1d) OVER w, 0),
            0.00000001
        ) AS idx_ret_1d_n,
        (idx_ret_5d - AVG(idx_ret_5d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(idx_ret_5d) OVER w, 0),
            0.00000001
        ) AS idx_ret_5d_n,
        (idx_close_pos - AVG(idx_close_pos) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(idx_close_pos) OVER w, 0),
            0.00000001
        ) AS idx_close_pos_n,
        (idx_range - AVG(idx_range) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(idx_range) OVER w, 0),
            0.00000001
        ) AS idx_range_n,
        (idx_vol_20 - AVG(idx_vol_20) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(idx_vol_20) OVER w, 0),
            0.00000001
        ) AS idx_vol_20_n,
        (idx_value_z_n - AVG(idx_value_z_n) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(idx_value_z_n) OVER w, 0),
            0.00000001
        ) AS idx_value_z_n_n,
        -- Currency Exchange Rate features
        (
            currency_exchange_rate_daily_return - AVG(currency_exchange_rate_daily_return) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(currency_exchange_rate_daily_return) OVER w,
                0
            ),
            0.00000001
        ) AS currency_exchange_rate_daily_return_n,
        (
            currency_exchange_rate_log_return - AVG(currency_exchange_rate_log_return) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(currency_exchange_rate_log_return) OVER w,
                0
            ),
            0.00000001
        ) AS currency_exchange_rate_log_return_n,
        (
            currency_exchange_rate_ma_7 - AVG(currency_exchange_rate_ma_7) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(currency_exchange_rate_ma_7) OVER w,
                0
            ),
            0.00000001
        ) AS currency_exchange_rate_ma_7_n,
        (
            currency_exchange_rate_ma_30 - AVG(currency_exchange_rate_ma_30) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(currency_exchange_rate_ma_30) OVER w,
                0
            ),
            0.00000001
        ) AS currency_exchange_rate_ma_30_n,
        (
            currency_exchange_rate_volatility_7 - AVG(currency_exchange_rate_volatility_7) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(currency_exchange_rate_volatility_7) OVER w,
                0
            ),
            0.00000001
        ) AS currency_exchange_rate_volatility_7_n,
        (
            currency_exchange_rate_dist_from_ma30 - AVG(currency_exchange_rate_dist_from_ma30) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(currency_exchange_rate_dist_from_ma30) OVER w,
                0
            ),
            0.00000001
        ) AS currency_exchange_rate_dist_from_ma30_n,
        -- time features
        dow_sin,
        dow_cos,
        woy_sin,
        woy_cos,
        month_sin,
        month_cos,
        ROW_NUMBER() OVER (
            PARTITION BY stock_profile
            ORDER BY
                timestamp
        ) AS step_count
    FROM
        stock_base WINDOW w AS (
            PARTITION BY stock_profile
            ORDER BY
                timestamp ROWS BETWEEN 60 PRECEDING
                AND 1 PRECEDING
        )
)
SELECT
    *
FROM
    base
WHERE
    step_count > 60;

DROP TABLE IF EXISTS stock_return_normalized;

CREATE TEMPORARY TABLE stock_return_normalized AS
SELECT
    sdn.stock_profile,
    sdn.timestamp,
    sdn.zero_volume_count,
    sdn.ret_1d_n,
    sdn.ret_5d_n,
    sdn.ret_20d_n,
    sdn.gap_n,
    sdn.intraday_range_n,
    sdn.close_position,
    sdn.vol_20_n,
    sdn.drawdown_n,
    sdn.turnover_n,
    sdn.foreign_flow_n,
    sdn.order_imbalance_n,
    sdn.spread_proxy_n,
    sdn.idx_ret_1d_n,
    sdn.idx_ret_5d_n,
    sdn.idx_close_pos_n,
    sdn.idx_range_n,
    sdn.idx_vol_20_n,
    sdn.idx_value_z_n_n,
    sdn.currency_exchange_rate_daily_return_n,
    sdn.currency_exchange_rate_log_return_n,
    sdn.currency_exchange_rate_ma_7_n,
    sdn.currency_exchange_rate_ma_30_n,
    sdn.currency_exchange_rate_volatility_7_n,
    sdn.currency_exchange_rate_dist_from_ma30_n,
    sdn.dow_sin,
    sdn.dow_cos,
    sdn.woy_sin,
    sdn.woy_cos,
    sdn.month_sin,
    sdn.month_cos,
    -- Target
    PERCENT_RANK() OVER (
        PARTITION BY srt.timestamp
        ORDER BY
            srt.future_return_5d
    ) AS future_return_5d,
    srt.future_volume_5d,
    ROW_NUMBER() OVER (
        PARTITION BY sdn.stock_profile
        ORDER BY
            sdn.timestamp DESC
    ) AS step_count,
    COUNT(*) OVER (PARTITION BY sdn.stock_profile) as total_step
FROM
    stock_data_normalized sdn
    INNER JOIN stock_return_target srt ON sdn.timestamp = srt.timestamp
    AND sdn.stock_profile = srt.stock_profile;

DROP TABLE IF EXISTS stock_vol_normalized;

CREATE TEMPORARY TABLE stock_vol_normalized AS
SELECT
    n.stock_profile,
    n.timestamp,
    n.zero_volume_count,
    n.ret_1d_n,
    n.ret_5d_n,
    n.ret_20d_n,
    n.vol_20_n,
    n.drawdown_n,
    n.turnover_n,
    n.foreign_flow_n,
    n.order_imbalance_n,
    n.spread_proxy_n,
    v.min_future_volume_20d,
    v.future_vol_20d,
    ROW_NUMBER() OVER (
        PARTITION BY n.stock_profile
        ORDER BY
            n.timestamp DESC
    ) AS step_count,
    COUNT(*) OVER (PARTITION BY n.stock_profile) as total_step
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
    n.spread_proxy_n,
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
    c.future_volume_5d,
    c.crash,
    ROW_NUMBER() OVER (
        PARTITION BY n.stock_profile
        ORDER BY
            n.timestamp DESC
    ) AS step_count,
    COUNT(*) OVER (PARTITION BY n.stock_profile) as total_step
FROM
    stock_base n
    JOIN stock_crash_target c ON n.stock_profile = c.stock_profile
    AND n.timestamp = c.timestamp;

DROP TABLE IF EXISTS stock_return_train;

DROP TABLE IF EXISTS stock_return_val;

CREATE TABLE stock_return_train AS
SELECT
    *
FROM
    stock_return_normalized
WHERE
    step_count > ROUND(total_step * @VAL_RATIO, 0);

CREATE TABLE stock_return_val AS
SELECT
    *
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
    zero_volume_count,
    ret_1d_n,
    ret_5d_n,
    ret_20d_n,
    vol_20_n,
    drawdown_n,
    turnover_n,
    foreign_flow_n,
    order_imbalance_n,
    spread_proxy_n,
    min_future_volume_20d,
    future_vol_20d
FROM
    stock_vol_normalized
WHERE
    step_count > ROUND(total_step * @VAL_RATIO, 0);

CREATE TABLE stock_vol_val AS
SELECT
    stock_profile,
    timestamp,
    zero_volume_count,
    ret_1d_n,
    ret_5d_n,
    ret_20d_n,
    vol_20_n,
    drawdown_n,
    turnover_n,
    foreign_flow_n,
    order_imbalance_n,
    spread_proxy_n,
    min_future_volume_20d,
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
    spread_proxy_n,
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
    spread_proxy_n,
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
    future_volume_5d,
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
    future_volume_5d,
    crash
FROM
    stock_crash_normalized
WHERE
    step_count <= ROUND(total_step * @VAL_RATIO, 0);