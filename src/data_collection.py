# src/data_collection.py
import os
import pandas as pd
from src.db_connect import RedshiftClient
from src.utils import load_config

class DataCollector:
    def __init__(self, config_path="config.yaml"):
        self.client = RedshiftClient(config_path)
        self.config = load_config(config_path)

    def _convert_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert problematic dtypes (categorical/object) to string for Parquet compatibility.
        """
        for col in df.columns:
            if pd.api.types.is_categorical_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
                df[col] = df[col].astype(str)
        return df

    def _save_dataframe(self, df: pd.DataFrame, path: str):
        """
        Save DataFrame to Parquet at given path.
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        print(f"Saved data: {path}")

    def collect_and_store(self, merge_on="user_id", how="inner") -> pd.DataFrame:
        """
        Fetch all training SQLs, store raw results, merge them, and store processed dataset.
        Returns merged DataFrame.
        """
        sql_dir = self.client.sql_dirs['training']
        raw_dir = self.config['data']['training']['raw']
        processed_dir = self.config['data']['training']['processed']

        merged_df = None

        for file in os.listdir(sql_dir):
            if file.endswith(".sql"):
                query_name = file.replace(".sql", "")
                # Fetch SQL result
                df = self.client.run_query(file, mode="training")
                df = self._convert_dtypes(df)

                # Save raw
                raw_path = os.path.join(raw_dir, f"{query_name}.parquet")
                self._save_dataframe(df, raw_path)

                # Merge efficiently
                if merged_df is None:
                    merged_df = df
                else:
                    merged_df = merged_df.merge(df, on=merge_on, how=how)

        # Save merged/processed dataset
        processed_path = os.path.join(processed_dir, "merged_data.parquet")
        merged_df = self._convert_dtypes(merged_df)  # Ensure merged df is safe
        self._save_dataframe(merged_df, processed_path)

        # Close connection
        self.client.close()
        return merged_df


if __name__ == "__main__":
    collector = DataCollector()
    merged_data = collector.collect_and_store()
    print(merged_data.head())