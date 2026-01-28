import pandas as pd
import numpy as np

from config import (
    INITIAL_CAPITAL,
    HOLD_DAYS,
    TRANSACTION_COST,
)
from position_control import build_portfolio


def compute_drawdown(equity: pd.Series) -> pd.Series:
    peak = equity.cummax()
    return (equity - peak) / peak


def walk_forward_backtest(
    price_df: pd.DataFrame,
    alpha_df: pd.DataFrame,
    risk_df: pd.DataFrame,
) -> pd.DataFrame:

    for df in (price_df, alpha_df, risk_df):
        df["date"] = pd.to_datetime(df["date"]).dt.date

    trading_days = sorted(alpha_df["date"].unique())

    price_map = {
        (r.date, r.stock_profile): (r.open_price, r.close)
        for r in price_df.itertuples()
    }

    cash = INITIAL_CAPITAL
    positions = {}
    entry_prices = {}
    exit_day = None

    equity_curve = []
    dates = []

    for i, today in enumerate(trading_days):

        if exit_day is not None and today == exit_day:

            proceeds = 0.0
            cost = 0.0

            for stock, shares in positions.items():
                key = (today, stock)
                if key not in price_map:
                    continue

                _, close_price = price_map[key]
                proceeds += shares * close_price
                cost += abs(shares * close_price) * TRANSACTION_COST

            cash += proceeds - cost

            positions = {}
            entry_prices = {}
            exit_day = None

        equity = cash

        for stock, shares in positions.items():
            key = (today, stock)
            if key in price_map:
                _, close_price = price_map[key]
                equity += shares * close_price

        equity_curve.append(equity)
        dates.append(today)

        if positions:
            continue

        if i + HOLD_DAYS >= len(trading_days):
            break

        risk_row = risk_df[risk_df["date"] == today]
        if risk_row.empty:
            continue

        today_alpha = alpha_df[alpha_df["date"] == today]
        if today_alpha.empty:
            continue

        portfolio = build_portfolio(
            alpha_df=today_alpha,
            risk_state=risk_row.iloc[0]["risk_state"],
            risk_score=risk_row.iloc[0]["risk_exposure"],
        )

        if portfolio.empty:
            continue

        entry_day = trading_days[i + 1]
        exit_day = trading_days[i + HOLD_DAYS]

        invested = 0.0
        entry_cost = 0.0

        for _, row in portfolio.iterrows():
            stock = row["stock_profile"]
            weight = row["target_weight"]

            key = (entry_day, stock)
            if key not in price_map:
                continue

            open_price, _ = price_map[key]
            if open_price <= 0:
                continue

            allocation = cash * weight
            shares = allocation / open_price

            positions[stock] = shares
            entry_prices[stock] = open_price

            invested += allocation
            entry_cost += allocation * TRANSACTION_COST

        cash -= invested + entry_cost

        assert cash >= -1e-8, "Negative cash (leverage detected)"

    result = pd.DataFrame({
        "date": dates,
        "equity": equity_curve,
    })

    result["drawdown"] = compute_drawdown(result["equity"])

    return result


def summarize(result_df: pd.DataFrame) -> dict:
    equity = result_df["equity"]

    total_return = equity.iloc[-1] / equity.iloc[0] - 1.0
    max_dd = result_df["drawdown"].min()

    returns = equity.pct_change()
    active = returns[returns != 0]

    sharpe = (
        np.sqrt(252) * active.mean() / active.std()
        if len(active) > 1 and active.std() > 0 else 0.0
    )

    return {
        "Total Return": total_return,
        "Max Drawdown": max_dd,
        "Sharpe": sharpe,
    }
