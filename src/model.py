import pandas as pd
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.tree import DecisionTreeRegressor, DecisionTreeClassifier
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from xgboost import XGBRegressor, XGBClassifier

class ModelTrainer:
    def __init__(self, data_path="src/data/training/processed/final_training_data.csv"):
        self.data_path = data_path
        self.df = None
        self.models = {}

    # ------------------- Load Data -------------------
    def load_data(self):
        self.df = pd.read_csv(self.data_path)

        # Drop identifier/date columns
        self.df.drop(columns=["user_id", "reference_date"], inplace=True, errors="ignore")

    # ------------------- Split Features/Targets -------------------
    def get_features_targets(self):
        X = self.df.drop(columns=["t__ccf", "t__is_drawn"], errors="ignore")
        y_reg = self.df["t__ccf"]
        y_clf = self.df["t__is_drawn"]
        return X, y_reg, y_clf

    # ------------------- Fit Regression Models -------------------
    def fit_regression_models(self, X, y):
        reg_models = {
            "linear_regression": LinearRegression(),
            "decision_tree": DecisionTreeRegressor(random_state=42),
            "random_forest": RandomForestRegressor(n_estimators=100, random_state=42),
            "xgboost": XGBRegressor(n_estimators=100, random_state=42, eval_metric="rmse")
        }

        fitted = {}
        for name, model in reg_models.items():
            model.fit(X, y)
            fitted[name] = model
        return fitted

    # ------------------- Fit Classification Models -------------------
    def fit_classification_models(self, X, y):
        clf_models = {
            "logistic_regression": LogisticRegression(max_iter=1000),
            "decision_tree": DecisionTreeClassifier(random_state=42),
            "random_forest": RandomForestClassifier(n_estimators=100, random_state=42),
            "xgboost": XGBClassifier(n_estimators=100, random_state=42, eval_metric="logloss")
        }

        fitted = {}
        for name, model in clf_models.items():
            model.fit(X, y)
            fitted[name] = model
        return fitted

    # ------------------- Full Pipeline -------------------
    def train_all(self):
        self.load_data()
        X, y_reg, y_clf = self.get_features_targets()

        print("Training regression models for t__ccf...")
        self.models["regression"] = self.fit_regression_models(X, y_reg)

        print("Training classification models for t__is_drawn...")
        self.models["classification"] = self.fit_classification_models(X, y_clf)

        print("All models trained.")
        return self.models


# ------------------- Script Mode -------------------
if __name__ == "__main__":
    trainer = ModelTrainer()
    models = trainer.train_all()

    # Example: access a fitted model
    # models["regression"]["linear_regression"]
