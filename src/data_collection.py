import os
import pandas as pd
from src.db_connect import RedshiftClient


class DataCollector:
    """
    Collects feature SQLs from Redshift, stores individual outputs,
    merges them left-joining on user_id and reference_date using sample.sql as base,
    and stores merged dataset.
    """

    def __init__(self, mode: str = "training", base_sql: str = "sample.sql"):
        self.mode = mode
        self.raw_dir = os.path.join("src", "data", self.mode, "raw")
        self.client = RedshiftClient()
        self.sql_dir = self.client.sql_dirs[self.mode]
        self.base_sql = base_sql

        os.makedirs(self.raw_dir, exist_ok=True)

    def collect_all_features(self) -> dict[str, pd.DataFrame]:
        """Run all SQL files and store each result as CSV. Returns a dict of DataFrames."""
        dataframes = {}
        sql_files = [f for f in os.listdir(self.sql_dir) if f.endswith(".sql")]

        for sql_file in sql_files:
            print(f"Running: {sql_file}")
            df = self.client.run_query(sql_file, mode=self.mode)
            out_path = os.path.join(self.raw_dir, sql_file.replace(".sql", ".csv"))
            df.to_csv(out_path, index=False)
            print(f"Saved: {out_path}")
            dataframes[sql_file] = df

        return dataframes

    def merge_features_with_base(self, dataframes: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Merge all feature DataFrames onto sample.sql left-joining by user_id and reference_date."""
        if self.base_sql not in dataframes:
            raise ValueError(f"Base SQL file '{self.base_sql}' not found in collected data.")

        merged_df = dataframes[self.base_sql]

        for sql_file, df in dataframes.items():
            if sql_file == self.base_sql:
                continue
            merged_df = pd.merge(
                merged_df,
                df,
                on=["user_id", "reference_date"],
                how="left"
            )

        merged_path = os.path.join(self.raw_dir, f"merged_{self.mode}_data.csv")
        merged_df.to_csv(merged_path, index=False)
        print(f"Merged dataset saved to {merged_path} (shape: {merged_df.shape})")
        return merged_df

    def run(self) -> pd.DataFrame:
        """Full pipeline: collect → store → merge → store."""
        dataframes = self.collect_all_features()
        merged_df = self.merge_features_with_base(dataframes)
        self.client.close()
        print("Connection closed.")
        return merged_df


if __name__ == "__main__":
    collector = DataCollector(mode="training", base_sql="sample.sql")
    df = collector.run()
    print(f"Final dataset shape: {df.shape}")
