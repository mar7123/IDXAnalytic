from xgboost import XGBClassifier


def train_crash_model(X, y):
    model = XGBClassifier(
        max_depth=4,
        n_estimators=300,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8
    )
    print("Train Crash")
    model.fit(X, y)
    return model
