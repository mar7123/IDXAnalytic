SET
    @WINDOW := 60;

SET
    @HORIZON := 5;

SET
    @VAL_RATIO := 0.2;

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

SELECT
    @MIN_DATE := MIN(timestamp)
FROM
    (
        SELECT
            DISTINCT timestamp
        FROM
            stock_data
        ORDER BY
            timestamp DESC
        LIMIT
            500
    ) t;

DROP TABLE IF EXISTS market_base;

CREATE TEMPORARY TABLE market_base AS WITH index_base AS (
    SELECT
        timestamp,
        CASE
            WHEN highest = lowest THEN 0.5
            ELSE (close - lowest) / (highest - lowest)
        END AS idx_close_pos,
        highest,
        lowest,
        value AS idx_value
    FROM
        index_data
    WHERE
        index_profile = "COMPOSITE"
        AND timestamp >= @MIN_DATE
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
merged_timestamp_base AS (
    SELECT
        ib.*,
        ceb.currency_exchange_rate
    FROM
        index_base ib
        INNER JOIN currency_exchange_base ceb ON ib.timestamp = ceb.timestamp
),
base AS (
    SELECT
        timestamp,
        -- Index features
        idx_close_pos,
        (highest - lowest) / LAG(idx_value, 1) OVER w20 AS idx_range,
        LN(idx_value / LAG(idx_value, 1) OVER w20) AS idx_ret_1d,
        LN(idx_value / LAG(idx_value, 5) OVER w20) AS idx_ret_5d,
        LN(idx_value / LAG(idx_value, 20) OVER w20) AS idx_ret_20d,
        LN(idx_value / LAG(idx_value, 60) OVER w60) AS idx_ret_60d,
        (idx_value / MAX(idx_value) OVER w20) - 1 AS idx_drawdown_20d,
        (idx_value / MAX(idx_value) OVER w60) - 1 AS idx_drawdown_60d,
        -- Currency features
        LN(
            currency_exchange_rate / LAG(currency_exchange_rate, 1) OVER w20
        ) AS currency_exchange_rate_ret_1d,
        LN(
            currency_exchange_rate / LAG(currency_exchange_rate, 5) OVER w20
        ) AS currency_exchange_rate_ret_5d,
        LN(
            currency_exchange_rate / LAG(currency_exchange_rate, 20) OVER w20
        ) AS currency_exchange_rate_ret_20d,
        LN(
            currency_exchange_rate / LAG(currency_exchange_rate, 60) OVER w60
        ) AS currency_exchange_rate_ret_60d
    FROM
        merged_timestamp_base WINDOW w20 AS (
            ORDER BY
                timestamp ROWS BETWEEN 19 PRECEDING
                AND CURRENT ROW
        ),
        w60 AS (
            ORDER BY
                timestamp ROWS BETWEEN 59 PRECEDING
                AND CURRENT ROW
        )
),
window_base AS (
    SELECT
        *,
        LN(STDDEV_SAMP(idx_ret_1d) OVER w20) AS idx_vol_20d,
        LN(STDDEV_SAMP(idx_ret_1d) OVER w60) AS idx_vol_60d,
        LN(
            STDDEV_SAMP(currency_exchange_rate_ret_1d) OVER w20
        ) AS currency_exchange_rate_vol_20d,
        LN(
            STDDEV_SAMP(currency_exchange_rate_ret_1d) OVER w60
        ) AS currency_exchange_rate_vol_60d,
        currency_exchange_rate_ret_1d - AVG(currency_exchange_rate_ret_1d) OVER w20 AS currency_exchange_rate_mr_20d,
        currency_exchange_rate_ret_1d - AVG(currency_exchange_rate_ret_1d) OVER w60 AS currency_exchange_rate_mr_60d
    FROM
        base WINDOW w20 AS (
            ORDER BY
                timestamp ROWS BETWEEN 19 PRECEDING
                AND CURRENT ROW
        ),
        w60 AS (
            ORDER BY
                timestamp ROWS BETWEEN 59 PRECEDING
                AND CURRENT ROW
        )
)
SELECT
    *
FROM
    window_base
WHERE
    idx_ret_60d IS NOT NULL;

DROP TABLE IF EXISTS stock_base;

CREATE TEMPORARY TABLE stock_base AS WITH base as (
    SELECT
        stock_profile,
        timestamp,
        close,
        -- suspension check
        volume,
        -- derived features
        LN(1 + (volume / NULLIF(tradeble_shares, 0))) AS turnover,
        (foreign_buy - foreign_sell) / NULLIF(volume, 0) AS foreign_flow,
        (bid_volume - offer_volume) / NULLIF(bid_volume + offer_volume, 0) AS order_imbalance,
        (offer - bid) / NULLIF(close, 0) AS spread_proxy,
        -- returns
        LN((close / LAG(close, 1) OVER w20)) AS ret_1d,
        LN((close / LAG(close, 5) OVER w20)) AS ret_5d,
        LN((close / LAG(close, 20) OVER w20)) AS ret_20d,
        LN((close / LAG(close, 60) OVER w60)) AS ret_60d,
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
                WHEN open_price = 0
                AND previous != 0 THEN previous
                ELSE open_price
            END - previous
        ) / NULLIF(previous, 0) AS gap,
        (high - low) / NULLIF(previous, 0) AS intraday_range,
        CASE
            WHEN high != low THEN (close - low) / NULLIF(high - low, 0)
            ELSE 0.5
        END AS close_position
    FROM
        stock_data
    WHERE
        timestamp >= @MIN_DATE WINDOW w20 AS (
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
        )
),
window_base AS (
    SELECT
        *,
        (close / MAX(close) OVER w20) - 1 AS drawdown_20d,
        (close / MAX(close) OVER w60) - 1 AS drawdown_60d,
        LN(STDDEV_SAMP(ret_1d) OVER w20 + 0.00000001) AS vol_20d,
        LN(STDDEV_SAMP(ret_1d) OVER w60 + 0.00000001) AS vol_60d
    FROM
        base WINDOW w20 AS (
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
        )
)
SELECT
    wb.*,
    mb.idx_close_pos,
    mb.idx_range,
    mb.idx_ret_1d,
    mb.idx_ret_5d,
    mb.idx_ret_20d,
    mb.idx_ret_60d,
    mb.currency_exchange_rate_ret_1d,
    mb.currency_exchange_rate_ret_5d,
    mb.currency_exchange_rate_ret_20d,
    mb.currency_exchange_rate_ret_60d,
    mb.idx_vol_20d,
    mb.idx_vol_60d,
    mb.idx_drawdown_20d,
    mb.idx_drawdown_60d,
    mb.currency_exchange_rate_vol_20d,
    mb.currency_exchange_rate_vol_60d,
    mb.currency_exchange_rate_mr_20d,
    mb.currency_exchange_rate_mr_60d,
    (wb.ret_1d - mb.idx_ret_1d) AS relative_ret_1d,
    (wb.ret_5d - mb.idx_ret_5d) AS relative_ret_5d,
    (wb.ret_20d - mb.idx_ret_20d) AS relative_ret_20d,
    (wb.ret_60d - mb.idx_ret_60d) AS relative_ret_60d
FROM
    window_base wb
    INNER JOIN market_base mb ON wb.timestamp = mb.timestamp
WHERE
    ret_60d IS NOT NULL;

DROP TABLE IF EXISTS model_target;

CREATE TEMPORARY TABLE model_target AS WITH base AS (
    SELECT
        stock_profile,
        timestamp,
        -- step counter
        COUNT(*) OVER (PARTITION BY stock_profile) as total_step,
        -- suspension check
        (LEAD(volume, @HORIZON) OVER w) AS future_volume,
        MAX(
            CASE
                WHEN volume = 0 THEN 1
                ELSE 0
            END
        ) OVER w AS zero_future_volume,
        -- future target
        LN(LEAD(close, @HORIZON) OVER w / close) AS future_return,
        LN(STDDEV_SAMP(ret_1d) OVER w + 0.00000001) AS future_vol,
        LN(MIN(close) OVER w / close) AS future_drawdown
    FROM
        stock_base WINDOW w AS (
            PARTITION BY stock_profile
            ORDER BY
                timestamp ROWS BETWEEN 1 FOLLOWING
                AND 5 FOLLOWING
        )
)
SELECT
    *,
    CASE
        WHEN future_return < LN(0.92) THEN 1
        WHEN future_return > LN(1.08) THEN 2
        ELSE 0
    END AS future_regime
FROM
    base
WHERE
    future_return IS NOT NULL
    AND ROUND(total_step * @VAL_RATIO) > (@WINDOW + @HORIZON + 1);

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
        (ret_60d - AVG(ret_60d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(ret_60d) OVER w, 0),
            0.00000001
        ) AS ret_60d_n,
        (gap - AVG(gap) OVER w) / COALESCE(NULLIF(STDDEV_SAMP(gap) OVER w, 0), 0.00000001) AS gap_n,
        (intraday_range - AVG(intraday_range) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(intraday_range) OVER w, 0),
            0.00000001
        ) AS intraday_range_n,
        close_position,
        (drawdown_20d - AVG(drawdown_20d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(drawdown_20d) OVER w, 0),
            0.00000001
        ) AS drawdown_20d_n,
        (drawdown_60d - AVG(drawdown_60d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(drawdown_60d) OVER w, 0),
            0.00000001
        ) AS drawdown_60d_n,
        (vol_20d - AVG(vol_20d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(vol_20d) OVER w, 0),
            0.00000001
        ) AS vol_20d_n,
        (vol_60d - AVG(vol_60d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(vol_60d) OVER w, 0),
            0.00000001
        ) AS vol_60d_n,
        idx_close_pos,
        (idx_range - AVG(idx_range) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(idx_range) OVER w, 0),
            0.00000001
        ) AS idx_range_n,
        (idx_ret_1d - AVG(idx_ret_1d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(idx_ret_1d) OVER w, 0),
            0.00000001
        ) AS idx_ret_1d_n,
        (idx_ret_5d - AVG(idx_ret_5d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(idx_ret_5d) OVER w, 0),
            0.00000001
        ) AS idx_ret_5d_n,
        (idx_ret_20d - AVG(idx_ret_20d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(idx_ret_20d) OVER w, 0),
            0.00000001
        ) AS idx_ret_20d_n,
        (idx_ret_60d - AVG(idx_ret_60d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(idx_ret_60d) OVER w, 0),
            0.00000001
        ) AS idx_ret_60d_n,
        (
            currency_exchange_rate_ret_1d - AVG(currency_exchange_rate_ret_1d) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(currency_exchange_rate_ret_1d) OVER w,
                0
            ),
            0.00000001
        ) AS currency_exchange_rate_ret_1d_n,
        (
            currency_exchange_rate_ret_5d - AVG(currency_exchange_rate_ret_5d) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(currency_exchange_rate_ret_5d) OVER w,
                0
            ),
            0.00000001
        ) AS currency_exchange_rate_ret_5d_n,
        (
            currency_exchange_rate_ret_20d - AVG(currency_exchange_rate_ret_20d) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(currency_exchange_rate_ret_20d) OVER w,
                0
            ),
            0.00000001
        ) AS currency_exchange_rate_ret_20d_n,
        (
            currency_exchange_rate_ret_60d - AVG(currency_exchange_rate_ret_60d) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(currency_exchange_rate_ret_60d) OVER w,
                0
            ),
            0.00000001
        ) AS currency_exchange_rate_ret_60d_n,
        (idx_vol_20d - AVG(idx_vol_20d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(idx_vol_20d) OVER w, 0),
            0.00000001
        ) AS idx_vol_20d_n,
        (idx_vol_60d - AVG(idx_vol_60d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(idx_vol_60d) OVER w, 0),
            0.00000001
        ) AS idx_vol_60d_n,
        (idx_drawdown_20d - AVG(idx_drawdown_20d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(idx_drawdown_20d) OVER w, 0),
            0.00000001
        ) AS idx_drawdown_20d_n,
        (idx_drawdown_60d - AVG(idx_drawdown_60d) OVER w) / COALESCE(
            NULLIF(STDDEV_SAMP(idx_drawdown_60d) OVER w, 0),
            0.00000001
        ) AS idx_drawdown_60d_n,
        (
            currency_exchange_rate_vol_20d - AVG(currency_exchange_rate_vol_20d) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(currency_exchange_rate_vol_20d) OVER w,
                0
            ),
            0.00000001
        ) AS currency_exchange_rate_vol_20d_n,
        (
            currency_exchange_rate_vol_60d - AVG(currency_exchange_rate_vol_60d) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(currency_exchange_rate_vol_60d) OVER w,
                0
            ),
            0.00000001
        ) AS currency_exchange_rate_vol_60d_n,
        (
            currency_exchange_rate_mr_20d - AVG(currency_exchange_rate_mr_20d) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(currency_exchange_rate_mr_20d) OVER w,
                0
            ),
            0.00000001
        ) AS currency_exchange_rate_mr_20d_n,
        (
            currency_exchange_rate_mr_60d - AVG(currency_exchange_rate_mr_60d) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(currency_exchange_rate_mr_60d) OVER w,
                0
            ),
            0.00000001
        ) AS currency_exchange_rate_mr_60d_n,
        (
            relative_ret_1d - AVG(relative_ret_1d) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(relative_ret_1d) OVER w,
                0
            ),
            0.00000001
        ) AS relative_ret_1d_n,
        (
            relative_ret_5d - AVG(relative_ret_5d) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(relative_ret_5d) OVER w,
                0
            ),
            0.00000001
        ) AS relative_ret_5d_n,
        (
            relative_ret_20d - AVG(relative_ret_20d) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(relative_ret_20d) OVER w,
                0
            ),
            0.00000001
        ) AS relative_ret_20d_n,
        (
            relative_ret_60d - AVG(relative_ret_60d) OVER w
        ) / COALESCE(
            NULLIF(
                STDDEV_SAMP(relative_ret_60d) OVER w,
                0
            ),
            0.00000001
        ) AS relative_ret_60d_n,
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
    stock_profile,
    timestamp,
    zero_volume_count,
    turnover_n,
    foreign_flow_n,
    order_imbalance_n,
    spread_proxy_n,
    ret_1d_n,
    ret_5d_n,
    ret_20d_n,
    ret_60d_n,
    gap_n,
    intraday_range_n,
    close_position,
    drawdown_20d_n,
    drawdown_60d_n,
    vol_20d_n,
    vol_60d_n,
    idx_close_pos,
    idx_range_n,
    idx_ret_1d_n,
    idx_ret_5d_n,
    idx_ret_20d_n,
    idx_ret_60d_n,
    currency_exchange_rate_ret_1d_n,
    currency_exchange_rate_ret_5d_n,
    currency_exchange_rate_ret_20d_n,
    currency_exchange_rate_ret_60d_n,
    idx_vol_20d_n,
    idx_vol_60d_n,
    idx_drawdown_20d_n,
    idx_drawdown_60d_n,
    currency_exchange_rate_vol_20d_n,
    currency_exchange_rate_vol_60d_n,
    currency_exchange_rate_mr_20d_n,
    currency_exchange_rate_mr_60d_n,
    relative_ret_1d_n,
    relative_ret_5d_n,
    relative_ret_20d_n,
    relative_ret_60d_n,
    dow_sin,
    dow_cos,
    woy_sin,
    woy_cos,
    month_sin,
    month_cos
FROM
    base
WHERE
    step_count > 60;

DROP TABLE IF EXISTS stock_return_normalized;

CREATE TEMPORARY TABLE stock_return_normalized AS
SELECT
    sdn.*,
    ROW_NUMBER() OVER (
        PARTITION BY sdn.stock_profile
        ORDER BY
            sdn.timestamp DESC
    ) AS step_count,
    COUNT(*) OVER (PARTITION BY sdn.stock_profile) as total_step,
    -- Suspension Check
    mt.future_volume,
    -- Target
    (
        mt.future_return - AVG(mt.future_return) OVER wt
    ) / COALESCE(
        NULLIF(
            STDDEV(mt.future_return) OVER wt,
            0
        ),
        0.00000001
    ) AS future_return
FROM
    stock_data_normalized sdn
    INNER JOIN model_target mt ON sdn.timestamp = mt.timestamp
    AND sdn.stock_profile = mt.stock_profile WINDOW wt AS (PARTITION BY mt.timestamp);

DROP TABLE IF EXISTS stock_vol_normalized;

CREATE TEMPORARY TABLE stock_vol_normalized AS
SELECT
    sdn.*,
    ROW_NUMBER() OVER (
        PARTITION BY sdn.stock_profile
        ORDER BY
            sdn.timestamp DESC
    ) AS step_count,
    COUNT(*) OVER (PARTITION BY sdn.stock_profile) as total_step,
    -- Suspension Check
    mt.zero_future_volume,
    -- Target
    (
        mt.future_vol - AVG(mt.future_vol) OVER wt
    ) / COALESCE(
        NULLIF(
            STDDEV(mt.future_vol) OVER wt,
            0
        ),
        0.00000001
    ) AS future_vol
FROM
    stock_data_normalized sdn
    JOIN model_target mt ON sdn.stock_profile = mt.stock_profile
    AND sdn.timestamp = mt.timestamp WINDOW wt AS (PARTITION BY mt.timestamp);

DROP TABLE IF EXISTS stock_drawdown_normalized;

CREATE TEMPORARY TABLE stock_drawdown_normalized AS
SELECT
    sdn.*,
    ROW_NUMBER() OVER (
        PARTITION BY sdn.stock_profile
        ORDER BY
            sdn.timestamp DESC
    ) AS step_count,
    COUNT(*) OVER (PARTITION BY sdn.stock_profile) as total_step,
    -- Suspension Check
    mt.zero_future_volume,
    -- Target
    (
        mt.future_drawdown - AVG(mt.future_drawdown) OVER wt
    ) / COALESCE(
        NULLIF(
            STDDEV(mt.future_drawdown) OVER wt,
            0
        ),
        0.00000001
    ) AS future_drawdown
FROM
    stock_data_normalized sdn
    JOIN model_target mt ON sdn.stock_profile = mt.stock_profile
    AND sdn.timestamp = mt.timestamp WINDOW wt AS (PARTITION BY mt.timestamp);

DROP TABLE IF EXISTS stock_regime_normalized;

CREATE TEMPORARY TABLE stock_regime_normalized AS
SELECT
    sdn.*,
    ROW_NUMBER() OVER (
        PARTITION BY sdn.stock_profile
        ORDER BY
            sdn.timestamp DESC
    ) AS step_count,
    COUNT(*) OVER (PARTITION BY sdn.stock_profile) AS total_step,
    -- Suspension Check
    mt.future_volume,
    -- Target
    mt.future_regime
FROM
    stock_data_normalized sdn
    JOIN model_target mt ON sdn.stock_profile = mt.stock_profile
    AND sdn.timestamp = mt.timestamp;

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
    *
FROM
    stock_vol_normalized
WHERE
    step_count > ROUND(total_step * @VAL_RATIO, 0);

CREATE TABLE stock_vol_val AS
SELECT
    *
FROM
    stock_vol_normalized
WHERE
    step_count <= ROUND(total_step * @VAL_RATIO, 0);

DROP TABLE IF EXISTS stock_drawdown_train;

DROP TABLE IF EXISTS stock_drawdown_val;

CREATE TABLE stock_drawdown_train AS
SELECT
    *
FROM
    stock_drawdown_normalized
WHERE
    step_count > ROUND(total_step * @VAL_RATIO, 0);

CREATE TABLE stock_drawdown_val AS
SELECT
    *
FROM
    stock_drawdown_normalized
WHERE
    step_count <= ROUND(total_step * @VAL_RATIO, 0);

DROP TABLE IF EXISTS stock_regime_train;

DROP TABLE IF EXISTS stock_regime_val;

CREATE TABLE stock_regime_train AS
SELECT
    *
FROM
    stock_regime_normalized
WHERE
    step_count > ROUND(total_step * @VAL_RATIO, 0);

CREATE TABLE stock_regime_val AS
SELECT
    *
FROM
    stock_regime_normalized
WHERE
    step_count <= ROUND(total_step * @VAL_RATIO, 0);

DROP TABLE IF EXISTS stock_inference;

CREATE TABLE stock_inference AS WITH base AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY stock_profile
            ORDER BY
                timestamp DESC
        ) AS step_count
    FROM
        stock_data_normalized
)
SELECT
    *
FROM
    base
WHERE
    step_count <= @WINDOW;