import pandas as pd

from config import OUTPUT_PATH
from data_loader import get_engine
from market_risk import run_market_risk
from alpha_ranking import run_alpha_ranking
from position_control import build_portfolio
from utils import write_log
from walkforward_backtest import walk_forward_backtest, summarize


def main():
    db = get_engine()

    write_log("Running market risk gate...")
    risk_df = run_market_risk(db)

    write_log("Running alpha ranking...")
    alpha_df = run_alpha_ranking(db)

    latest_date = alpha_df["date"].max()

    today_alpha = alpha_df[alpha_df["date"] == latest_date]
    today_risk = risk_df[risk_df["date"] == latest_date].iloc[0]

    portfolio_df = build_portfolio(
        alpha_df=today_alpha,
        risk_state=today_risk["risk_state"],
        risk_score=today_risk["risk_exposure"]
    )
    portfolio_df["date"] = latest_date

    write_log("Running walk-forward backtest...")

    price_df = pd.read_sql(
        """
        SELECT
            date,
            stock_profile,
            open_price,
            close
        FROM feature_store_daily
        ORDER BY date
        """,
        con=db
    )

    backtest_df = walk_forward_backtest(
        price_df=price_df,
        alpha_df=alpha_df,
        risk_df=risk_df
    )

    summary = summarize(backtest_df)

    summary_df = pd.DataFrame(
        summary.items(),
        columns=["Metric", "Value"]
    )

    write_log(f"Exporting results to {OUTPUT_PATH}")

    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        risk_df.to_excel(writer, sheet_name="risk_gate", index=False)
        alpha_df.to_excel(writer, sheet_name="alpha_ranking", index=False)
        portfolio_df.to_excel(writer, sheet_name="portfolio", index=False)
        backtest_df.to_excel(writer, sheet_name="equity_curve", index=False)
        summary_df.to_excel(writer, sheet_name="summary", index=False)

    write_log("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
