USE idx;

DROP TABLE IF EXISTS index_daily_base;

CREATE TABLE index_daily_base AS WITH last_timestamps as (
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
    volume
FROM
    index_data
WHERE
    index_profile = 'COMPOSITE'
    AND timestamp >= (
        SELECT
            MIN(timestamp)
        from
            last_timestamps
    );

DROP TABLE IF EXISTS index_returns;

CREATE TABLE index_returns AS
SELECT
    *,
    close / LAG(close, 1) OVER w - 1 AS ret_1d,
    close / LAG(close, 3) OVER w - 1 AS ret_3d,
    close / LAG(close, 5) OVER w - 1 AS ret_5d
FROM
    index_daily_base WINDOW w AS (
        PARTITION BY index_profile
        ORDER BY
            date
    );

DROP TABLE IF EXISTS index_rolling_features;

CREATE TABLE index_rolling_features AS
SELECT
    *,
    STDDEV(ret_1d) OVER (
        ORDER BY
            date ROWS BETWEEN 4 PRECEDING
            AND CURRENT ROW
    ) AS vol_5d,
    STDDEV(ret_1d) OVER (
        ORDER BY
            date ROWS BETWEEN 9 PRECEDING
            AND CURRENT ROW
    ) AS vol_10d,
    LOG(
        volume / NULLIF(
            AVG(volume) OVER (
                ORDER BY
                    date ROWS BETWEEN 9 PRECEDING
                    AND CURRENT ROW
            ),
            0
        )
    ) AS volume_ratio_10d
FROM
    index_returns;

DROP TABLE IF EXISTS index_risk_labeled;

CREATE TABLE index_risk_labeled AS
SELECT
    *,
    LEAD(close, 3) OVER w / close - 1 AS fwd_ret_3d,
    CASE
        WHEN LEAD(close, 3) OVER w / close - 1 > 0 THEN 1
        ELSE 0
    END AS risk_on_label
FROM
    index_rolling_features WINDOW w AS (
        ORDER BY
            date
    );

DELETE FROM
    index_risk_labeled
WHERE
    ret_5d IS NULL
    OR vol_10d IS NULL
    OR fwd_ret_3d IS NULL;