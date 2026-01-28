USE idx;

DROP TABLE IF EXISTS alpha_rank_dataset;

CREATE TABLE alpha_rank_dataset AS WITH base AS (
    SELECT
        date,
        stock_profile,
        fwd_ret_3d,
        ret_1d,
        ret_3d,
        ret_5d,
        volatility_5d,
        volatility_10d,
        volume_ratio_10d,
        bid_offer_imbalance,
        foreign_flow_ratio,
        non_regular_vol_ratio,
        hl_range,
        gap_open
    FROM
        feature_store_daily
    WHERE
        fwd_ret_3d IS NOT NULL
),
valid_dates AS (
    SELECT
        date
    FROM
        base
    GROUP BY
        date
    HAVING
        COUNT(*) >= 5
),
labeled AS (
    SELECT
        b.*,
        NTILE(5) OVER (
            PARTITION BY b.date
            ORDER BY
                b.fwd_ret_3d,
                b.stock_profile
        ) - 1 AS label
    FROM
        base b
        JOIN valid_dates v ON b.date = v.date
)
SELECT
    date,
    stock_profile,
    label,
    ret_1d,
    ret_3d,
    ret_5d,
    volatility_5d,
    volatility_10d,
    volume_ratio_10d,
    bid_offer_imbalance,
    foreign_flow_ratio,
    non_regular_vol_ratio,
    hl_range,
    gap_open
FROM
    labeled;