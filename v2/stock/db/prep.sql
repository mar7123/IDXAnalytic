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
        CASE
            WHEN s.open_price = 0
            AND s.previous != 0 THEN s.previous
            ELSE s.open_price
        END AS open_price,
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
        close / MAX(close) OVER w120 - 1 AS drawdown,
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
        w120 AS (
            PARTITION BY stock_profile
            ORDER BY
                timestamp ROWS BETWEEN 119 PRECEDING
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
    AND wb.step_count < (wb.total_step -120);

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

CREATE TEMPORARY TABLE stock_data_normalized AS
SELECT
    stock_profile,
    timestamp,
    -- Suspension check
    volume,
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
    -- Index features
    idx_ret_1d,
    idx_ret_5d,
    idx_close_pos,
    idx_range,
    idx_vol_20,
    idx_value_z_n,
    -- Currency Exchange Rate features
    currency_exchange_rate_daily_return,
    currency_exchange_rate_log_return,
    currency_exchange_rate_ma_7,
    currency_exchange_rate_ma_30,
    currency_exchange_rate_volatility_7,
    currency_exchange_rate_dist_from_ma30
FROM
    stock_base;

DROP TABLE IF EXISTS stock_return_normalized;

CREATE TEMPORARY TABLE stock_return_normalized AS
SELECT
    sdn.*,
    PERCENT_RANK() OVER (
        PARTITION BY timestamp
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
    n.volume,
    n.ret_1d_n,
    n.ret_5d_n,
    n.ret_20d_n,
    n.vol_20_n,
    n.drawdown_n,
    n.turnover_n,
    n.foreign_flow_n,
    n.order_imbalance_n,
    n.spread_n,
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

DROP TABLE IF EXISTS stock_return_index_scaler;

CREATE TEMPORARY TABLE stock_return_index_scaler WITH ranked_data AS (
    SELECT
        stock_profile,
        -- Index features
        idx_ret_1d,
        idx_ret_5d,
        idx_close_pos,
        idx_range,
        idx_vol_20,
        idx_value_z_n,
        -- Currency Exchange Rate features
        currency_exchange_rate_daily_return,
        currency_exchange_rate_log_return,
        currency_exchange_rate_ma_7,
        currency_exchange_rate_ma_30,
        currency_exchange_rate_volatility_7,
        currency_exchange_rate_dist_from_ma30,
        -- Index features
        PERCENT_RANK() OVER (
            PARTITION BY stock_profile
            ORDER BY
                idx_ret_1d
        ) AS p_rank_idx_ret_1d,
        PERCENT_RANK() OVER (
            PARTITION BY stock_profile
            ORDER BY
                idx_ret_5d
        ) AS p_rank_idx_ret_5d,
        PERCENT_RANK() OVER (
            PARTITION BY stock_profile
            ORDER BY
                idx_close_pos
        ) AS p_rank_idx_close_pos,
        PERCENT_RANK() OVER (
            PARTITION BY stock_profile
            ORDER BY
                idx_range
        ) AS p_rank_idx_range,
        PERCENT_RANK() OVER (
            PARTITION BY stock_profile
            ORDER BY
                idx_vol_20
        ) AS p_rank_idx_vol_20,
        PERCENT_RANK() OVER (
            PARTITION BY stock_profile
            ORDER BY
                idx_value_z_n
        ) AS p_rank_idx_value_z_n,
        -- Currency Exchange Rate features
        PERCENT_RANK() OVER (
            PARTITION BY stock_profile
            ORDER BY
                currency_exchange_rate_daily_return
        ) AS p_rank_currency_exchange_rate_daily_return,
        PERCENT_RANK() OVER (
            PARTITION BY stock_profile
            ORDER BY
                currency_exchange_rate_log_return
        ) AS p_rank_currency_exchange_rate_log_return,
        PERCENT_RANK() OVER (
            PARTITION BY stock_profile
            ORDER BY
                currency_exchange_rate_ma_7
        ) AS p_rank_currency_exchange_rate_ma_7,
        PERCENT_RANK() OVER (
            PARTITION BY stock_profile
            ORDER BY
                currency_exchange_rate_ma_30
        ) AS p_rank_currency_exchange_rate_ma_30,
        PERCENT_RANK() OVER (
            PARTITION BY stock_profile
            ORDER BY
                currency_exchange_rate_volatility_7
        ) AS p_rank_currency_exchange_rate_volatility_7,
        PERCENT_RANK() OVER (
            PARTITION BY stock_profile
            ORDER BY
                currency_exchange_rate_dist_from_ma30
        ) AS p_rank_currency_exchange_rate_dist_from_ma30
    FROM
        stock_return_normalized
    WHERE
        step_count > ROUND(total_step * @VAL_RATIO, 0)
)
SELECT
    stock_profile,
    -- Index features
    MAX(
        CASE
            WHEN p_rank_idx_ret_1d <= 0.25 THEN idx_ret_1d
        END
    ) AS q1_idx_ret_1d,
    MAX(
        CASE
            WHEN p_rank_idx_ret_1d <= 0.50 THEN idx_ret_1d
        END
    ) AS median_idx_ret_1d,
    MAX(
        CASE
            WHEN p_rank_idx_ret_1d <= 0.75 THEN idx_ret_1d
        END
    ) AS q3_idx_ret_1d,
    MAX(
        CASE
            WHEN p_rank_idx_ret_5d <= 0.25 THEN idx_ret_5d
        END
    ) AS q1_idx_ret_5d,
    MAX(
        CASE
            WHEN p_rank_idx_ret_5d <= 0.50 THEN idx_ret_5d
        END
    ) AS median_idx_ret_5d,
    MAX(
        CASE
            WHEN p_rank_idx_ret_5d <= 0.75 THEN idx_ret_5d
        END
    ) AS q3_idx_ret_5d,
    MAX(
        CASE
            WHEN p_rank_idx_close_pos <= 0.25 THEN idx_close_pos
        END
    ) AS q1_idx_close_pos,
    MAX(
        CASE
            WHEN p_rank_idx_close_pos <= 0.50 THEN idx_close_pos
        END
    ) AS median_idx_close_pos,
    MAX(
        CASE
            WHEN p_rank_idx_close_pos <= 0.75 THEN idx_close_pos
        END
    ) AS q3_idx_close_pos,
    MAX(
        CASE
            WHEN p_rank_idx_range <= 0.25 THEN idx_range
        END
    ) AS q1_idx_range,
    MAX(
        CASE
            WHEN p_rank_idx_range <= 0.50 THEN idx_range
        END
    ) AS median_idx_range,
    MAX(
        CASE
            WHEN p_rank_idx_range <= 0.75 THEN idx_range
        END
    ) AS q3_idx_range,
    MAX(
        CASE
            WHEN p_rank_idx_vol_20 <= 0.25 THEN idx_vol_20
        END
    ) AS q1_idx_vol_20,
    MAX(
        CASE
            WHEN p_rank_idx_vol_20 <= 0.50 THEN idx_vol_20
        END
    ) AS median_idx_vol_20,
    MAX(
        CASE
            WHEN p_rank_idx_vol_20 <= 0.75 THEN idx_vol_20
        END
    ) AS q3_idx_vol_20,
    MAX(
        CASE
            WHEN p_rank_idx_value_z_n <= 0.25 THEN idx_value_z_n
        END
    ) AS q1_idx_value_z_n,
    MAX(
        CASE
            WHEN p_rank_idx_value_z_n <= 0.50 THEN idx_value_z_n
        END
    ) AS median_idx_value_z_n,
    MAX(
        CASE
            WHEN p_rank_idx_value_z_n <= 0.75 THEN idx_value_z_n
        END
    ) AS q3_idx_value_z_n,
    -- Currency Exchange Rate features
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_daily_return <= 0.25 THEN currency_exchange_rate_daily_return
        END
    ) AS q1_currency_exchange_rate_daily_return,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_daily_return <= 0.50 THEN currency_exchange_rate_daily_return
        END
    ) AS median_currency_exchange_rate_daily_return,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_daily_return <= 0.75 THEN currency_exchange_rate_daily_return
        END
    ) AS q3_currency_exchange_rate_daily_return,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_log_return <= 0.25 THEN currency_exchange_rate_log_return
        END
    ) AS q1_currency_exchange_rate_log_return,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_log_return <= 0.50 THEN currency_exchange_rate_log_return
        END
    ) AS median_currency_exchange_rate_log_return,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_log_return <= 0.75 THEN currency_exchange_rate_log_return
        END
    ) AS q3_currency_exchange_rate_log_return,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_ma_7 <= 0.25 THEN currency_exchange_rate_ma_7
        END
    ) AS q1_currency_exchange_rate_ma_7,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_ma_7 <= 0.50 THEN currency_exchange_rate_ma_7
        END
    ) AS median_currency_exchange_rate_ma_7,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_ma_7 <= 0.75 THEN currency_exchange_rate_ma_7
        END
    ) AS q3_currency_exchange_rate_ma_7,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_ma_30 <= 0.25 THEN currency_exchange_rate_ma_30
        END
    ) AS q1_currency_exchange_rate_ma_30,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_ma_30 <= 0.50 THEN currency_exchange_rate_ma_30
        END
    ) AS median_currency_exchange_rate_ma_30,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_ma_30 <= 0.75 THEN currency_exchange_rate_ma_30
        END
    ) AS q3_currency_exchange_rate_ma_30,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_volatility_7 <= 0.25 THEN currency_exchange_rate_volatility_7
        END
    ) AS q1_currency_exchange_rate_volatility_7,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_volatility_7 <= 0.50 THEN currency_exchange_rate_volatility_7
        END
    ) AS median_currency_exchange_rate_volatility_7,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_volatility_7 <= 0.75 THEN currency_exchange_rate_volatility_7
        END
    ) AS q3_currency_exchange_rate_volatility_7,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_dist_from_ma30 <= 0.25 THEN currency_exchange_rate_dist_from_ma30
        END
    ) AS q1_currency_exchange_rate_dist_from_ma30,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_dist_from_ma30 <= 0.50 THEN currency_exchange_rate_dist_from_ma30
        END
    ) AS median_currency_exchange_rate_dist_from_ma30,
    MAX(
        CASE
            WHEN p_rank_currency_exchange_rate_dist_from_ma30 <= 0.75 THEN currency_exchange_rate_dist_from_ma30
        END
    ) AS q3_currency_exchange_rate_dist_from_ma30
FROM
    ranked_data
GROUP BY
    stock_profile;

DROP TABLE IF EXISTS stock_return_train;

DROP TABLE IF EXISTS stock_return_val;

CREATE TABLE stock_return_train AS
SELECT
    srn.stock_profile,
    srn.timestamp,
    srn.volume,
    -- normalized inputs
    srn.ret_1d_n,
    srn.ret_5d_n,
    srn.ret_20d_n,
    srn.gap_n,
    srn.intraday_range_n,
    srn.close_position,
    srn.vol_20_n,
    srn.drawdown_n,
    srn.turnover_n,
    srn.foreign_flow_n,
    srn.order_imbalance_n,
    srn.spread_n,
    srn.dow_sin,
    srn.dow_cos,
    srn.woy_sin,
    srn.woy_cos,
    srn.month_sin,
    srn.month_cos,
    -- Index features
    (srn.idx_ret_1d - sris.median_idx_ret_1d) / NULLIF(sris.q3_idx_ret_1d - sris.q1_idx_ret_1d, 0) AS idx_ret_1d_n,
    (srn.idx_ret_5d - sris.median_idx_ret_5d) / NULLIF(sris.q3_idx_ret_5d - sris.q1_idx_ret_5d, 0) AS idx_ret_5d_n,
    (srn.idx_close_pos - sris.median_idx_close_pos) / NULLIF(sris.q3_idx_close_pos - sris.q1_idx_close_pos, 0) AS idx_close_pos_n,
    (srn.idx_range - sris.median_idx_range) / NULLIF(sris.q3_idx_range - sris.q1_idx_range, 0) AS idx_range_n,
    (srn.idx_vol_20 - sris.median_idx_vol_20) / NULLIF(sris.q3_idx_vol_20 - sris.q1_idx_vol_20, 0) AS idx_vol_20_n,
    (srn.idx_value_z_n - sris.median_idx_value_z_n) / NULLIF(sris.q3_idx_value_z_n - sris.q1_idx_value_z_n, 0) AS idx_value_z_n_n,
    -- Currency Exchange Rate features
    (
        srn.currency_exchange_rate_daily_return - sris.median_currency_exchange_rate_daily_return
    ) / NULLIF(
        sris.q3_currency_exchange_rate_daily_return - q1_currency_exchange_rate_daily_return,
        0
    ) AS currency_exchange_rate_daily_return_n,
    (
        srn.currency_exchange_rate_log_return - sris.median_currency_exchange_rate_log_return
    ) / NULLIF(
        sris.q3_currency_exchange_rate_log_return - q1_currency_exchange_rate_log_return,
        0
    ) AS currency_exchange_rate_log_return_n,
    (
        srn.currency_exchange_rate_ma_7 - sris.median_currency_exchange_rate_ma_7
    ) / NULLIF(
        sris.q3_currency_exchange_rate_ma_7 - q1_currency_exchange_rate_ma_7,
        0
    ) AS currency_exchange_rate_ma_7_n,
    (
        srn.currency_exchange_rate_ma_30 - sris.median_currency_exchange_rate_ma_30
    ) / NULLIF(
        sris.q3_currency_exchange_rate_ma_30 - q1_currency_exchange_rate_ma_30,
        0
    ) AS currency_exchange_rate_ma_30_n,
    (
        srn.currency_exchange_rate_volatility_7 - sris.median_currency_exchange_rate_volatility_7
    ) / NULLIF(
        sris.q3_currency_exchange_rate_volatility_7 - q1_currency_exchange_rate_volatility_7,
        0
    ) AS currency_exchange_rate_volatility_7_n,
    (
        srn.currency_exchange_rate_dist_from_ma30 - sris.median_currency_exchange_rate_dist_from_ma30
    ) / NULLIF(
        sris.q3_currency_exchange_rate_dist_from_ma30 - q1_currency_exchange_rate_dist_from_ma30,
        0
    ) AS currency_exchange_rate_dist_from_ma30_n,
    -- target
    srn.future_return_5d,
    srn.future_volume_5d
FROM
    stock_return_normalized srn
    INNER JOIN stock_return_index_scaler sris ON srn.stock_profile = sris.stock_profile
WHERE
    srn.step_count > ROUND(srn.total_step * @VAL_RATIO, 0);

CREATE TABLE stock_return_val AS
SELECT
    srn.stock_profile,
    srn.timestamp,
    srn.volume,
    -- normalized inputs
    srn.ret_1d_n,
    srn.ret_5d_n,
    srn.ret_20d_n,
    srn.gap_n,
    srn.intraday_range_n,
    srn.close_position,
    srn.vol_20_n,
    srn.drawdown_n,
    srn.turnover_n,
    srn.foreign_flow_n,
    srn.order_imbalance_n,
    srn.spread_n,
    srn.dow_sin,
    srn.dow_cos,
    srn.woy_sin,
    srn.woy_cos,
    srn.month_sin,
    srn.month_cos,
    -- Index features
    (srn.idx_ret_1d - sris.median_idx_ret_1d) / NULLIF(sris.q3_idx_ret_1d - sris.q1_idx_ret_1d, 0) AS idx_ret_1d_n,
    (srn.idx_ret_5d - sris.median_idx_ret_5d) / NULLIF(sris.q3_idx_ret_5d - sris.q1_idx_ret_5d, 0) AS idx_ret_5d_n,
    (srn.idx_close_pos - sris.median_idx_close_pos) / NULLIF(sris.q3_idx_close_pos - sris.q1_idx_close_pos, 0) AS idx_close_pos_n,
    (srn.idx_range - sris.median_idx_range) / NULLIF(sris.q3_idx_range - sris.q1_idx_range, 0) AS idx_range_n,
    (srn.idx_vol_20 - sris.median_idx_vol_20) / NULLIF(sris.q3_idx_vol_20 - sris.q1_idx_vol_20, 0) AS idx_vol_20_n,
    (srn.idx_value_z_n - sris.median_idx_value_z_n) / NULLIF(sris.q3_idx_value_z_n - sris.q1_idx_value_z_n, 0) AS idx_value_z_n_n,
    -- Currency Exchange Rate features
    (
        srn.currency_exchange_rate_daily_return - sris.median_currency_exchange_rate_daily_return
    ) / NULLIF(
        sris.q3_currency_exchange_rate_daily_return - q1_currency_exchange_rate_daily_return,
        0
    ) AS currency_exchange_rate_daily_return_n,
    (
        srn.currency_exchange_rate_log_return - sris.median_currency_exchange_rate_log_return
    ) / NULLIF(
        sris.q3_currency_exchange_rate_log_return - q1_currency_exchange_rate_log_return,
        0
    ) AS currency_exchange_rate_log_return_n,
    (
        srn.currency_exchange_rate_ma_7 - sris.median_currency_exchange_rate_ma_7
    ) / NULLIF(
        sris.q3_currency_exchange_rate_ma_7 - q1_currency_exchange_rate_ma_7,
        0
    ) AS currency_exchange_rate_ma_7_n,
    (
        srn.currency_exchange_rate_ma_30 - sris.median_currency_exchange_rate_ma_30
    ) / NULLIF(
        sris.q3_currency_exchange_rate_ma_30 - q1_currency_exchange_rate_ma_30,
        0
    ) AS currency_exchange_rate_ma_30_n,
    (
        srn.currency_exchange_rate_volatility_7 - sris.median_currency_exchange_rate_volatility_7
    ) / NULLIF(
        sris.q3_currency_exchange_rate_volatility_7 - q1_currency_exchange_rate_volatility_7,
        0
    ) AS currency_exchange_rate_volatility_7_n,
    (
        srn.currency_exchange_rate_dist_from_ma30 - sris.median_currency_exchange_rate_dist_from_ma30
    ) / NULLIF(
        sris.q3_currency_exchange_rate_dist_from_ma30 - q1_currency_exchange_rate_dist_from_ma30,
        0
    ) AS currency_exchange_rate_dist_from_ma30_n,
    -- target
    srn.future_return_5d,
    srn.future_volume_5d
FROM
    stock_return_normalized srn
    INNER JOIN stock_return_index_scaler sris ON srn.stock_profile = sris.stock_profile
WHERE
    srn.step_count <= ROUND(srn.total_step * @VAL_RATIO, 0);

DROP TABLE IF EXISTS stock_vol_train;

DROP TABLE IF EXISTS stock_vol_val;

CREATE TABLE stock_vol_train AS
SELECT
    stock_profile,
    timestamp,
    volume,
    ret_1d_n,
    ret_5d_n,
    ret_20d_n,
    vol_20_n,
    drawdown_n,
    turnover_n,
    foreign_flow_n,
    order_imbalance_n,
    spread_n,
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
    volume,
    ret_1d_n,
    ret_5d_n,
    ret_20d_n,
    vol_20_n,
    drawdown_n,
    turnover_n,
    foreign_flow_n,
    order_imbalance_n,
    spread_n,
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