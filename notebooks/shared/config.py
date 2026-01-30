"""
ACA Demand Planning - Configuration Module

Usage in other notebooks:
    from config import CONFIG, DIO, STORAGE_CLIENT, run_bq_query, upload_to_bigquery
"""

import os
import io
from dataclasses import dataclass
import pandas as pd

# Try to import GCP libraries
try:
    from google.cloud import storage
    from google.cloud import bigquery
    GCP_AVAILABLE = True
except ImportError:
    GCP_AVAILABLE = False
    print("GCP libraries not available, running in local mode")


@dataclass
class Config:
    """Central configuration."""

    # GCP Settings
    GCP_PROJECT: str = "mimetic-maxim-443710-s2"
    GCS_BUCKET: str = "demand_planning_aca"
    BQ_DATASET: str = "redai_demand_forecast"

    # GCS Paths (within bucket)
    GCS_RAW_DATA: str = "raw_data/"
    GCS_PROCESSED: str = "processed/"
    GCS_FEATURES: str = "features/"
    GCS_MODELS: str = "models/"
    GCS_PREDICTIONS: str = "predictions/"

    # BigQuery Tables
    BQ_TABLE_PREDICTIONS_SKU: str = "predictions_sku_all"
    BQ_TABLE_PREDICTIONS_CATEGORY: str = "predictions_category_all"
    BQ_TABLE_PREDICTIONS_CUSTOMER: str = "predictions_customer_all"
    BQ_TABLE_EVAL: str = "eval_all_versions"
    BQ_TABLE_MODEL_SELECTION: str = "model_selection"

    # Local paths (for local development/testing)
    LOCAL_DATA_DIR: str = "../"
    LOCAL_RAW_DATA: str = "../raw_data/"
    LOCAL_PROCESSED: str = "../features_v2/"
    LOCAL_MODELS: str = "../model_evaluation/"

    # Model parameters
    MIN_H1_WEEKS: int = 10                       # Minimum weeks for modeling
    HIGH_CONF_WEEKS: int = 20                    # Weeks for high confidence
    WMAPE_HIGH_THRESHOLD: float = 40.0           # WMAPE < 40% = HIGH confidence
    WMAPE_MEDIUM_THRESHOLD: float = 70.0         # WMAPE < 70% = MEDIUM confidence

    # Data split
    H1_END_WEEK: int = 26                        # H1 = W01-W26
    H2_START_WEEK: int = 27                      # H2 = W27-W52

    def gcs_uri(self, path: str) -> str:
        """Get full GCS URI."""
        return f"gs://{self.GCS_BUCKET}/{path}"

    def bq_table(self, table_name: str) -> str:
        """Get full BigQuery table reference."""
        return f"{self.GCP_PROJECT}.{self.BQ_DATASET}.{table_name}"


# Create global config instance
CONFIG = Config()

# Initialize GCP clients
STORAGE_CLIENT = None
BQ_CLIENT = None

if GCP_AVAILABLE:
    try:
        STORAGE_CLIENT = storage.Client(project=CONFIG.GCP_PROJECT)
        BQ_CLIENT = bigquery.Client(project=CONFIG.GCP_PROJECT)
        print(f"✓ GCP clients initialized (Project: {CONFIG.GCP_PROJECT})")
    except Exception as e:
        print(f"GCP authentication failed: {e}")
        print("Running in local mode. To use GCP:")
        print("  1. Run 'gcloud auth application-default login'")
        print("  2. Or set GOOGLE_APPLICATION_CREDENTIALS environment variable")


def list_gcs_files(prefix: str = "", suffix: str = "") -> list:
    """List files in GCS bucket with optional prefix/suffix filter."""
    if STORAGE_CLIENT is None:
        return []

    bucket = STORAGE_CLIENT.bucket(CONFIG.GCS_BUCKET)
    blobs = bucket.list_blobs(prefix=prefix)

    files = []
    for blob in blobs:
        if suffix and not blob.name.endswith(suffix):
            continue
        files.append({
            'name': blob.name,
            'size_mb': blob.size / (1024 * 1024) if blob.size else 0,
            'updated': blob.updated
        })

    return files


def read_csv_from_gcs(gcs_path: str) -> pd.DataFrame:
    """Read CSV from GCS."""
    if STORAGE_CLIENT is None:
        local_path = os.path.join(CONFIG.LOCAL_DATA_DIR, gcs_path)
        return pd.read_csv(local_path)
    return pd.read_csv(CONFIG.gcs_uri(gcs_path))


def upload_df_to_gcs(df: pd.DataFrame, gcs_path: str, format: str = 'csv') -> str:
    """Upload DataFrame to GCS."""
    if STORAGE_CLIENT is None:
        local_path = os.path.join(CONFIG.LOCAL_DATA_DIR, gcs_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        df.to_csv(local_path, index=False)
        print(f"✓ Saved locally: {local_path}")
        return local_path

    bucket = STORAGE_CLIENT.bucket(CONFIG.GCS_BUCKET)
    blob = bucket.blob(gcs_path)

    if format == 'csv':
        blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
    elif format == 'parquet':
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        blob.upload_from_string(buffer.getvalue(), content_type='application/octet-stream')

    uri = CONFIG.gcs_uri(gcs_path)
    print(f"✓ Uploaded DataFrame ({len(df):,} rows) → {uri}")
    return uri


def run_bq_query(query: str) -> pd.DataFrame:
    """Run BigQuery query."""
    if BQ_CLIENT is None:
        raise ConnectionError("BigQuery not available")
    return BQ_CLIENT.query(query).to_dataframe()


def upload_to_bigquery(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    """Upload DataFrame to BigQuery."""
    if BQ_CLIENT is None:
        print(f"BigQuery not available, skipping upload to {table_name}")
        return None

    table_ref = CONFIG.bq_table(table_name)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if if_exists == "replace"
                         else bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True
    )
    job = BQ_CLIENT.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"✓ Uploaded {len(df):,} rows to BigQuery: {table_ref}")
    return table_ref


class DataIO:
    """
    Unified data I/O that automatically selects GCS or local storage.

    Usage:
        dio = DataIO()
        df = dio.read_csv('processed/v2_fact_lineitem.csv')
        dio.write_csv(df, 'processed/cleaned_data.csv')
        dio.write_to_bigquery(df, 'my_table')
    """

    def __init__(self, prefer_gcs: bool = True):
        self.use_gcs = prefer_gcs and STORAGE_CLIENT is not None
        self.use_bq = BQ_CLIENT is not None

        mode = "GCS + BigQuery" if self.use_gcs and self.use_bq else \
               "GCS only" if self.use_gcs else \
               "BigQuery only" if self.use_bq else "Local only"
        print(f"DataIO initialized: {mode}")

    def read_csv(self, path: str) -> pd.DataFrame:
        """Read CSV from GCS or local."""
        return read_csv_from_gcs(path)

    def read_excel(self, path: str) -> pd.DataFrame:
        """Read Excel from GCS or local."""
        if self.use_gcs:
            bucket = STORAGE_CLIENT.bucket(CONFIG.GCS_BUCKET)
            blob = bucket.blob(path)
            content = blob.download_as_bytes()
            return pd.read_excel(io.BytesIO(content))
        else:
            local_path = os.path.join(CONFIG.LOCAL_DATA_DIR, path)
            return pd.read_excel(local_path)

    def write_csv(self, df: pd.DataFrame, path: str, also_local: bool = False):
        """Write CSV to GCS and optionally local."""
        upload_df_to_gcs(df, path, format='csv')

        if also_local and self.use_gcs:
            local_path = os.path.join(CONFIG.LOCAL_DATA_DIR, path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            df.to_csv(local_path, index=False)
            print(f"✓ Also saved locally: {local_path}")

    def write_to_bigquery(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
        """Write DataFrame to BigQuery."""
        upload_to_bigquery(df, table_name, if_exists)

    def read_from_bigquery(self, table_name: str, limit: int = None) -> pd.DataFrame:
        """Read from BigQuery."""
        table_ref = CONFIG.bq_table(table_name)
        query = f"SELECT * FROM `{table_ref}`"
        if limit:
            query += f" LIMIT {limit}"
        return run_bq_query(query)

    def list_files(self, prefix: str = "") -> list:
        """List files in GCS or local directory."""
        if self.use_gcs:
            return list_gcs_files(prefix)
        else:
            local_dir = os.path.join(CONFIG.LOCAL_DATA_DIR, prefix)
            if os.path.exists(local_dir):
                return [{'name': f, 'size_mb': os.path.getsize(os.path.join(local_dir, f))/(1024*1024)}
                        for f in os.listdir(local_dir) if os.path.isfile(os.path.join(local_dir, f))]
            return []


# Create global DataIO instance
DIO = DataIO(prefer_gcs=True)
