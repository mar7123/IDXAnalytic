SELECT
    @vol_bull := 0.02;

SELECT
    @vol_bear := 0.03;

SELECT
    @crash_threshold := -0.03;

CREATE TEMPORARY TABLE whole_data WITH base_data AS (
    SELECT
        timestamp,
        close,
        volume,
        DAYOFWEEK(timestamp) AS dow,
        WEEKOFYEAR(timestamp) AS week_of_year,
        MONTH(timestamp) AS month,
        LAG(close, 1) OVER (
            ORDER BY
                timestamp
        ) AS prev_close_1d,
        LAG(close, 5) OVER (
            ORDER BY
                timestamp
        ) AS prev_close_5d,
        LAG(volume, 1) OVER (
            ORDER BY
                timestamp
        ) AS prev_volume_1d
    FROM
        index_data
    WHERE
        index_profile = "COMPOSITE"
),
calculated_features AS (
    SELECT
        *,
        (close - prev_close_1d) / NULLIF(prev_close_1d, 0) AS ret_1d,
        (close - prev_close_5d) / NULLIF(prev_close_5d, 0) AS ret_5d,
        AVG(close) OVER (
            ORDER BY
                timestamp ROWS BETWEEN 19 PRECEDING
                AND CURRENT ROW
        ) AS ma_20,
        AVG(close) OVER (
            ORDER BY
                timestamp ROWS BETWEEN 59 PRECEDING
                AND CURRENT ROW
        ) AS ma_60,
        close / MAX(close) OVER (
            ORDER BY
                timestamp ROWS BETWEEN UNBOUNDED PRECEDING
                AND CURRENT ROW
        ) - 1 AS drawdown,
        (volume - prev_volume_1d) / NULLIF(prev_volume_1d, 0) AS volume_change
    FROM
        base_data
),
volatility_calc AS (
    SELECT
        *,
        STDDEV_SAMP(ret_1d) OVER (
            ORDER BY
                timestamp ROWS BETWEEN 19 PRECEDING
                AND CURRENT ROW
        ) AS vol_20,
        STDDEV_SAMP(ret_1d) OVER (
            ORDER BY
                timestamp ROWS BETWEEN 59 PRECEDING
                AND CURRENT ROW
        ) AS vol_60,
        (ma_20 - ma_60) AS trend
    FROM
        calculated_features
),
final_targets AS (
    SELECT
        *,
        LEAD(vol_20, 5) OVER (
            ORDER BY
                timestamp
        ) AS future_vol,
        CASE
            WHEN LEAD(ret_1d, 1) OVER (
                ORDER BY
                    timestamp
            ) < @crash_threshold THEN 1
            ELSE 0
        END AS crash,
        SIN(2 * PI() * dow / 7) AS dow_sin,
        COS(2 * PI() * dow / 7) AS dow_cos,
        SIN(2 * PI() * week_of_year / 52) AS woy_sin,
        COS(2 * PI() * week_of_year / 52) AS woy_cos,
        SIN(2 * PI() * month / 12) AS month_sin,
        COS(2 * PI() * month / 12) AS month_cos,
        ROW_NUMBER() OVER (
            ORDER BY
                timestamp
        ) AS row_num
    FROM
        volatility_calc
)
SELECT
    ret_1d,
    ret_5d,
    vol_20,
    vol_60,
    trend,
    drawdown,
    volume,
    volume_change,
    dow_sin,
    dow_cos,
    woy_sin,
    woy_cos,
    month_sin,
    month_cos,
    future_vol,
    crash,
    CASE
        WHEN trend > 0
        AND vol_20 < @vol_bull THEN 0
        WHEN trend < 0
        AND vol_20 > @vol_bear THEN 1
        ELSE 2
    END AS regime,
    ROW_NUMBER() OVER (
        ORDER BY
            timestamp
    ) AS row_idx
FROM
    final_targets
WHERE
    row_num > 60
    AND future_vol IS NOT NULL
ORDER BY
    timestamp;

SELECT
    @cutoff := MAX(row_idx) * 0.8
FROM
    whole_data;

DROP TABLE IF EXISTS risk_engine_training_data;

CREATE TEMPORARY TABLE risk_engine_training_data AS
SELECT
    *
FROM
    whole_data
WHERE
    row_idx <= @cutoff;

DROP TABLE IF EXISTS risk_engine_test_data;

CREATE TEMPORARY TABLE risk_engine_test_data AS
SELECT
    *
FROM
    whole_data
WHERE
    row_idx > @cutoff;

DROP TABLE IF EXISTS risk_engine_scaler;

CREATE TEMPORARY TABLE risk_engine_scaler WITH RankedData AS (
    SELECT
        ret_1d,
        ret_5d,
        vol_20,
        vol_60,
        trend,
        drawdown,
        volume,
        volume_change,
        PERCENT_RANK() OVER (
            ORDER BY
                ret_1d
        ) AS p_rank_ret_1d,
        PERCENT_RANK() OVER (
            ORDER BY
                ret_5d
        ) AS p_rank_ret_5d,
        PERCENT_RANK() OVER (
            ORDER BY
                vol_20
        ) AS p_rank_vol_20,
        PERCENT_RANK() OVER (
            ORDER BY
                vol_60
        ) AS p_rank_vol_60,
        PERCENT_RANK() OVER (
            ORDER BY
                trend
        ) AS p_rank_trend,
        PERCENT_RANK() OVER (
            ORDER BY
                drawdown
        ) AS p_rank_drawdown,
        PERCENT_RANK() OVER (
            ORDER BY
                volume
        ) AS p_rank_volume,
        PERCENT_RANK() OVER (
            ORDER BY
                volume_change
        ) AS p_rank_volume_change
    FROM
        risk_engine_training_data
)
SELECT
    MAX(
        CASE
            WHEN p_rank_ret_1d <= 0.25 THEN ret_1d
        END
    ) AS q1_ret_1d,
    MAX(
        CASE
            WHEN p_rank_ret_1d <= 0.50 THEN ret_1d
        END
    ) AS median_ret_1d,
    MAX(
        CASE
            WHEN p_rank_ret_1d <= 0.75 THEN ret_1d
        END
    ) AS q3_ret_1d,
    MAX(
        CASE
            WHEN p_rank_ret_5d <= 0.25 THEN ret_5d
        END
    ) AS q1_ret_5d,
    MAX(
        CASE
            WHEN p_rank_ret_5d <= 0.50 THEN ret_5d
        END
    ) AS median_ret_5d,
    MAX(
        CASE
            WHEN p_rank_ret_5d <= 0.75 THEN ret_5d
        END
    ) AS q3_ret_5d,
    MAX(
        CASE
            WHEN p_rank_vol_20 <= 0.25 THEN vol_20
        END
    ) AS q1_vol_20,
    MAX(
        CASE
            WHEN p_rank_vol_20 <= 0.50 THEN vol_20
        END
    ) AS median_vol_20,
    MAX(
        CASE
            WHEN p_rank_vol_20 <= 0.75 THEN vol_20
        END
    ) AS q3_vol_20,
    MAX(
        CASE
            WHEN p_rank_vol_60 <= 0.25 THEN vol_60
        END
    ) AS q1_vol_60,
    MAX(
        CASE
            WHEN p_rank_vol_60 <= 0.50 THEN vol_60
        END
    ) AS median_vol_60,
    MAX(
        CASE
            WHEN p_rank_vol_60 <= 0.75 THEN vol_60
        END
    ) AS q3_vol_60,
    MAX(
        CASE
            WHEN p_rank_trend <= 0.25 THEN trend
        END
    ) AS q1_trend,
    MAX(
        CASE
            WHEN p_rank_trend <= 0.50 THEN trend
        END
    ) AS median_trend,
    MAX(
        CASE
            WHEN p_rank_trend <= 0.75 THEN trend
        END
    ) AS q3_trend,
    MAX(
        CASE
            WHEN p_rank_drawdown <= 0.25 THEN drawdown
        END
    ) AS q1_drawdown,
    MAX(
        CASE
            WHEN p_rank_drawdown <= 0.50 THEN drawdown
        END
    ) AS median_drawdown,
    MAX(
        CASE
            WHEN p_rank_drawdown <= 0.75 THEN drawdown
        END
    ) AS q3_drawdown,
    MAX(
        CASE
            WHEN p_rank_volume <= 0.25 THEN volume
        END
    ) AS q1_volume,
    MAX(
        CASE
            WHEN p_rank_volume <= 0.50 THEN volume
        END
    ) AS median_volume,
    MAX(
        CASE
            WHEN p_rank_volume <= 0.75 THEN volume
        END
    ) AS q3_volume,
    MAX(
        CASE
            WHEN p_rank_volume_change <= 0.25 THEN volume_change
        END
    ) AS q1_volume_change,
    MAX(
        CASE
            WHEN p_rank_volume_change <= 0.50 THEN volume_change
        END
    ) AS median_volume_change,
    MAX(
        CASE
            WHEN p_rank_volume_change <= 0.75 THEN volume_change
        END
    ) AS q3_volume_change
FROM
    RankedData;

DROP TABLE IF EXISTS risk_engine_normalized_training_data;

CREATE TABLE risk_engine_normalized_training_data AS
SELECT
    (e.ret_1d - f.median_ret_1d) / NULLIF(f.q3_ret_1d - f.q1_ret_1d, 0) AS ret_1d,
    (e.ret_5d - f.median_ret_5d) / NULLIF(f.q3_ret_5d - f.q1_ret_5d, 0) AS ret_5d,
    (e.vol_20 - f.median_vol_20) / NULLIF(f.q3_vol_20 - f.q1_vol_20, 0) AS vol_20,
    (e.vol_60 - f.median_vol_60) / NULLIF(f.q3_vol_60 - f.q1_vol_60, 0) AS vol_60,
    (e.trend - f.median_trend) / NULLIF(f.q3_trend - f.q1_trend, 0) AS trend,
    (e.drawdown - f.median_drawdown) / NULLIF(f.q3_drawdown - f.q1_drawdown, 0) AS drawdown,
    (e.volume - f.median_volume) / NULLIF(f.q3_volume - f.q1_volume, 0) AS volume,
    (e.volume_change - f.median_volume_change) / NULLIF(f.q3_volume_change - f.q1_volume_change, 0) AS volume_change,
    e.dow_sin,
    e.dow_cos,
    e.woy_sin,
    e.woy_cos,
    e.month_sin,
    e.month_cos,
    e.future_vol,
    e.crash,
    e.regime
FROM
    risk_engine_training_data e
    CROSS JOIN risk_engine_scaler f
ORDER BY
    e.row_idx;

DROP TABLE IF EXISTS risk_engine_normalized_test_data;

CREATE TABLE risk_engine_normalized_test_data AS
SELECT
    (e.ret_1d - f.median_ret_1d) / NULLIF(f.q3_ret_1d - f.q1_ret_1d, 0) AS ret_1d,
    (e.ret_5d - f.median_ret_5d) / NULLIF(f.q3_ret_5d - f.q1_ret_5d, 0) AS ret_5d,
    (e.vol_20 - f.median_vol_20) / NULLIF(f.q3_vol_20 - f.q1_vol_20, 0) AS vol_20,
    (e.vol_60 - f.median_vol_60) / NULLIF(f.q3_vol_60 - f.q1_vol_60, 0) AS vol_60,
    (e.trend - f.median_trend) / NULLIF(f.q3_trend - f.q1_trend, 0) AS trend,
    (e.drawdown - f.median_drawdown) / NULLIF(f.q3_drawdown - f.q1_drawdown, 0) AS drawdown,
    (e.volume - f.median_volume) / NULLIF(f.q3_volume - f.q1_volume, 0) AS volume,
    (e.volume_change - f.median_volume_change) / NULLIF(f.q3_volume_change - f.q1_volume_change, 0) AS volume_change,
    e.dow_sin,
    e.dow_cos,
    e.woy_sin,
    e.woy_cos,
    e.month_sin,
    e.month_cos,
    e.future_vol,
    e.crash,
    e.regime
FROM
    risk_engine_test_data e
    CROSS JOIN risk_engine_scaler f
ORDER BY
    e.row_idx;