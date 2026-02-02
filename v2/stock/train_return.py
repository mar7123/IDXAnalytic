from keras.models import Model
from keras.layers import Input, Dense, Dropout, LayerNormalization
from keras.optimizers import Adam
from keras.losses import Huber
from tcn import TCN

from stock.config import WINDOW


def build_tcn_return_model(num_features: int):
    inputs = Input(shape=(WINDOW, num_features))

    x = TCN(
        nb_filters=32,
        kernel_size=3,
        dilations=[1, 2, 4, 8, 16],
        padding="causal",
        dropout_rate=0.15,
        return_sequences=False
    )(inputs)

    x = LayerNormalization()(x)
    x = Dense(32, activation="relu")(x)
    x = Dropout(0.2)(x)

    outputs = Dense(1, activation="linear")(x)

    model = Model(inputs, outputs)
    model.compile(
        optimizer=Adam(learning_rate=1e-4),
        loss=Huber(delta=1.0)
    )
    return model
