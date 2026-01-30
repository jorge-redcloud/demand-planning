"""
ACA Demand Planning - Setup Script

Add this at the top of any notebook:

    import sys
    sys.path.insert(0, '../shared')
    from setup import *

This imports all shared modules and configures the environment.
"""

import sys
import os

# Ensure shared folder is in path
SHARED_DIR = os.path.dirname(os.path.abspath(__file__))
if SHARED_DIR not in sys.path:
    sys.path.insert(0, SHARED_DIR)

# Import everything from config
from config import (
    CONFIG,
    DIO,
    STORAGE_CLIENT,
    BQ_CLIENT,
    list_gcs_files,
    read_csv_from_gcs,
    upload_df_to_gcs,
    upload_to_bigquery,
    run_bq_query,
    DataIO
)

# Import experiment tracker
from experiment_tracker import (
    ExperimentTracker,
    ExperimentConfig,
    EXPERIMENT_CONFIG,
    create_experiment,
    baseline_experiment,
    no_blackfriday_experiment,
    custom_experiment
)

# Common imports for notebooks
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# Pandas display settings
pd.set_option('display.max_columns', 50)
pd.set_option('display.float_format', '{:.2f}'.format)

print(f"✓ Setup complete")
print(f"  GCP Project: {CONFIG.GCP_PROJECT}")
print(f"  GCS Bucket:  gs://{CONFIG.GCS_BUCKET}/")
print(f"  Using GCS:   {STORAGE_CLIENT is not None}")
print(f"  Using BQ:    {BQ_CLIENT is not None}")
