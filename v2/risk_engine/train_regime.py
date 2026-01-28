from keras.models import Sequential
from keras.layers import LSTM, Dense, Dropout, LayerNormalization
from keras.regularizers import l2
from keras.callbacks import EarlyStopping
from keras.optimizers import Adam
import pandas as pd

from risk_engine.dataset import make_sequences
from risk_engine.config import ALL_FEATURES, REGIME_TARGET


def train_regime_model(train_df: pd.DataFrame, test_df: pd.DataFrame):
    X_train, y_train = make_sequences(train_df, ALL_FEATURES, REGIME_TARGET)
    X_test, y_test = make_sequences(test_df, ALL_FEATURES, REGIME_TARGET)

    model = Sequential([
        LSTM(64, return_sequences=False, recurrent_dropout=0.2),
        LayerNormalization(),
        Dropout(0.4),

        Dense(32, activation="relu", kernel_regularizer=l2(0.01)),
        Dropout(0.3),
        Dense(3, activation="softmax"),
    ])

    model.compile(
        optimizer=Adam(learning_rate=1e-4),
        loss="sparse_categorical_crossentropy",
        metrics=["accuracy"]
    )

    print("Train Regime")
    model.fit(
        X_train, y_train,
        validation_data=(X_test, y_test),
        epochs=100,
        batch_size=32,
        callbacks=[EarlyStopping(
            monitor='val_loss',
            patience=10,
            restore_best_weights=True
        )]
    )
    return model
