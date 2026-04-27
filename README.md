# Stock Market Analysis & Prediction Pipeline

An end-to-end Machine Learning project designed to forecast stock performance by analyzing historical patterns, index movements, and currency exchange rates. The system utilizes a hybrid modeling approach, combining Temporal Convolutional Networks (TCN), LSTMs, and LightGBM to predict returns, volatility, and drawdowns.

## ## System Architecture

The project implements a multi-target forecasting strategy, where three distinct aspects of stock behavior are modeled:

1.  **Return Prediction:** Forecasting future price movements.
2.  **Volatility Prediction:** Forecasting the expected risk/variation.
3.  **Drawdown Prediction:** Forecasting potential peak-to-trough declines.


### Key Components

* **Hybrid Modeling:** Each target is predicted using two different architectures:
    * **Deep Learning:** TCN (for returns) and LSTM (for volatility/drawdown) to capture complex temporal dependencies.
    * **Tree-based:** LightGBM Regressors to capture non-linear feature interactions and provide robust baselines.
* **Adaptive Huber Loss:** The pipeline dynamically calculates and updates the `delta` parameter for the Huber loss function based on the Mean Absolute Deviation (MAD) of residuals.
* **Feature Engineering:** Utilizes 60-day sliding windows incorporating:
    * **Stock-specific features:** Returns, volume, turnover, and order imbalance.
    * **Macro features:** Index performance and currency exchange rate fluctuations.
    * **Temporal features:** Cyclical encoding (Sine/Cosine) for Day of Week, Week of Year, and Month.

---

## ## Project Structure

* `stock.py`: The main orchestrator. Handles database interaction, model ensemble training (4-fold random seeding), and result aggregation.
* `dataset.py`: Data preprocessing logic. Converts raw DataFrames into sliding window sequences for Deep Learning and flattened structures for LightGBM.
* `train_return.py`: Implementation of the Return prediction model using **TCN (Temporal Convolutional Network)**.
* `train_vol.py` & `train_drawdown.py`: Implementation of risk-metric models using **LSTM (Long Short-Term Memory)**.
* `config.py`: Centralized configuration for feature columns, hyperparameters, and the global `ConfigManager`.

---

## ## Workflow

1.  **Data Preparation:** `prep_data` executes SQL scripts to clean and format raw market data into inference-ready tables.
2.  **Mapping:** Dynamic mapping of `stock_profile` strings into integer IDs for the neural network Embedding layers.
3.  **Ensemble Training:** The system runs multiple iterations with different random seeds. In each iteration:
    * Deep Learning and LightGBM models are trained for all three targets.
    * The Huber delta is updated iteratively to minimize the impact of outliers.
4.  **Rank-based Scoring:** * Predictions are converted to **percentile ranks**.
    * Volatility and Drawdown are ranked in reverse (lower is better).
    * A final `result_score` is averaged across all models and iterations.
5.  **Output:** Generates `output.xlsx` containing the final stock rankings, average scores, and standard deviation (to measure prediction confidence).

---

## ## Technical Stack

* **Frameworks:** TensorFlow/Keras, LightGBM.
* **Data Handling:** Pandas, NumPy, SQLAlchemy.
* **Model Layers:** TCN, LSTM, Embedding, LayerNormalization.
* **Loss Function:** Adaptive Huber Loss.

---

## ## How to Run

1.  Ensure your SQL environment is configured with the scripts located at `v2/stock/db/`.
2.  Install dependencies: `pip install tensorflow keras-tcn lightgbm pandas sqlalchemy openpyxl`.
3.  Execute the main process:
    ```python
    from sqlalchemy import create_engine
    from stock.stock import stock_main

    engine = create_engine("your_db_connection_string")
    stock_main(engine)
    ```