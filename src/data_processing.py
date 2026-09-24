import os
import pandas as pd
from sklearn.preprocessing import OrdinalEncoder


class TrainingDataProcessor:
    def __init__(
        self,
        raw_path="src/data/training/raw/merged_training_data.csv",
        output_path="src/data/training/processed/final_training_data.csv",
        target_col="t__ccf"
    ):
        self.raw_path = raw_path
        self.output_path = output_path
        self.encoder = None
        self.target_col = target_col

        # Exclude from processing
        self.exclude_cols = ["user_id", "reference_date", "default_date"]

    # ------------------- Load Data -------------------
    def load_data(self):
        return pd.read_csv(self.raw_path)

    # ------------------- Variable Type Detection -------------------
    def identify_variable_types(self, df):
        df_proc = df.drop(columns=self.exclude_cols, errors="ignore")

        numeric_cols = df_proc.select_dtypes(include=["int64", "float64"]).columns.tolist()

        binary_cols = [
            col for col in numeric_cols
            if df_proc[col].dropna().nunique() == 2
        ]

        continuous_cols = list(set(numeric_cols) - set(binary_cols))

        categorical_cols = df_proc.select_dtypes(include=["object"]).columns.tolist()

        return continuous_cols, binary_cols, categorical_cols

    # ------------------- Combined Imputation -------------------
    def impute_missing(self, df, continuous_cols, binary_cols, categorical_cols):
        # 1. Continuous → 0
        if continuous_cols:
            df[continuous_cols] = df[continuous_cols].fillna(0)

        # 2. Binary → mode
        for col in binary_cols:
            df[col] = df[col].fillna(df[col].mode()[0])

        # 3. Categorical → mode (AFTER encoding)
        for col in categorical_cols:
            df[col] = df[col].fillna(df[col].mode()[0])

        return df

    # ------------------- Ordinal Encoding -------------------
    def encode_categorical(self, df, categorical_cols):
        if len(categorical_cols) == 0:
            return df

        self.encoder = OrdinalEncoder()
        df[categorical_cols] = self.encoder.fit_transform(df[categorical_cols])

        return df

    # ------------------- Target Capping -------------------
    def cap_target(self, df):
        if self.target_col in df.columns:
            df[self.target_col] = df[self.target_col].clip(upper=3)
        return df

    # ------------------- Save Output -------------------
    def save_data(self, df):
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        df.to_csv(self.output_path, index=False)

    # ------------------- Full Pipeline -------------------
    def process(self):
        print("Loading data...")
        df = self.load_data()

        print("Identifying variable types...")
        continuous_cols, binary_cols, categorical_cols = self.identify_variable_types(df)

        # ------------------- Encoding before imputation -------------------
        print("Encoding categorical variables...")
        df = self.encode_categorical(df, categorical_cols)

        # ------------------- Unified Imputation -------------------
        #print("Imputing missing values (continuous=0, binary=mode, categorical=mode)...")
        #df = self.impute_missing(df, continuous_cols, binary_cols, categorical_cols)

        # ------------------- Cap target -------------------
        #print(f"Capping target column '{self.target_col}' at 0.99 quantile...")
        #df = self.cap_target(df)

        print(f"Saving processed dataset to {self.output_path}...")
        self.save_data(df)

        print("Processing complete.")
        return df


# ------------------- Script Mode -------------------
if __name__ == "__main__":
    processor = TrainingDataProcessor()
    processor.process()