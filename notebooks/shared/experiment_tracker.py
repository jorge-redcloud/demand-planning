"""
ACA Demand Planning - Experiment Tracking Module

Integrates with Vertex AI Experiments for model versioning and comparison.

Usage:
    from experiment_tracker import ExperimentTracker

    # Start experiment
    tracker = ExperimentTracker(experiment_name="v5_exclude_w47")
    tracker.start_run(run_name="sku_level")

    # Log parameters
    tracker.log_params({
        'model_version': 'V5',
        'exclude_w47': True,
        'min_h1_weeks': 10
    })

    # Log metrics
    tracker.log_metrics({
        'wmape_median': 36.3,
        'wmape_p25': 32.0,
        'wmape_p75': 41.0
    })

    # Save artifacts (model, predictions)
    tracker.log_artifact(predictions_df, 'predictions_sku.csv')

    # End run
    tracker.end_run()
"""

import os
import json
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
import pandas as pd

# Try to import Vertex AI
try:
    from google.cloud import aiplatform
    from google.cloud.aiplatform import Experiment, ExperimentRun
    VERTEX_AVAILABLE = True
except ImportError:
    VERTEX_AVAILABLE = False
    print("Vertex AI not available. Install with: pip install google-cloud-aiplatform")

from config import CONFIG, STORAGE_CLIENT, upload_df_to_gcs


@dataclass
class ExperimentConfig:
    """Configuration for experiment tracking."""

    # Vertex AI settings
    project: str = CONFIG.GCP_PROJECT
    location: str = "us-central1"  # Change to your preferred region
    staging_bucket: str = f"gs://{CONFIG.GCS_BUCKET}"

    # Experiment paths in GCS
    experiments_path: str = "experiments/"

    # BigQuery settings
    bq_experiments_dataset: str = "experiments"

    def get_experiment_gcs_path(self, experiment_name: str, run_name: str = None) -> str:
        """Get GCS path for experiment artifacts."""
        if run_name:
            return f"{self.experiments_path}{experiment_name}/{run_name}/"
        return f"{self.experiments_path}{experiment_name}/"


EXPERIMENT_CONFIG = ExperimentConfig()


class ExperimentTracker:
    """
    Tracks ML experiments with Vertex AI Experiments.

    Handles:
    - Parameter logging
    - Metric tracking
    - Artifact storage (models, predictions, plots)
    - Experiment comparison
    """

    def __init__(
        self,
        experiment_name: str,
        description: str = None,
        use_vertex: bool = True
    ):
        self.experiment_name = experiment_name
        self.description = description or f"Demand Planning Experiment: {experiment_name}"
        self.use_vertex = use_vertex and VERTEX_AVAILABLE

        self.current_run = None
        self.run_name = None
        self.start_time = None

        # Local tracking (always available)
        self.params = {}
        self.metrics = {}
        self.artifacts = []

        # Initialize Vertex AI if available
        if self.use_vertex:
            self._init_vertex()
        else:
            print(f"Running in local mode. Artifacts will be saved to GCS: "
                  f"gs://{CONFIG.GCS_BUCKET}/{EXPERIMENT_CONFIG.experiments_path}{experiment_name}/")

    def _init_vertex(self):
        """Initialize Vertex AI."""
        try:
            aiplatform.init(
                project=EXPERIMENT_CONFIG.project,
                location=EXPERIMENT_CONFIG.location,
                staging_bucket=EXPERIMENT_CONFIG.staging_bucket,
                experiment=self.experiment_name,
                experiment_description=self.description
            )
            print(f"✓ Vertex AI Experiment initialized: {self.experiment_name}")
        except Exception as e:
            print(f"Failed to initialize Vertex AI: {e}")
            self.use_vertex = False

    def start_run(self, run_name: str = None, resume: bool = False):
        """
        Start a new experiment run.

        Args:
            run_name: Name for this run (e.g., 'sku_v2', 'category_v3')
            resume: Whether to resume an existing run
        """
        self.run_name = run_name or f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.start_time = datetime.now()

        # Reset tracking
        self.params = {}
        self.metrics = {}
        self.artifacts = []

        if self.use_vertex:
            try:
                aiplatform.start_run(self.run_name, resume=resume)
                print(f"✓ Started run: {self.run_name}")
            except Exception as e:
                print(f"Failed to start Vertex run: {e}")
        else:
            print(f"Started local run: {self.run_name}")

        return self

    def log_params(self, params: Dict[str, Any]):
        """Log parameters for current run."""
        self.params.update(params)

        if self.use_vertex:
            try:
                aiplatform.log_params(params)
            except Exception as e:
                print(f"Failed to log params to Vertex: {e}")

        print(f"Logged params: {list(params.keys())}")

    def log_metrics(self, metrics: Dict[str, float]):
        """Log metrics for current run."""
        self.metrics.update(metrics)

        if self.use_vertex:
            try:
                aiplatform.log_metrics(metrics)
            except Exception as e:
                print(f"Failed to log metrics to Vertex: {e}")

        print(f"Logged metrics: {metrics}")

    def log_artifact(
        self,
        data: pd.DataFrame,
        filename: str,
        artifact_type: str = 'csv'
    ) -> str:
        """
        Save artifact (DataFrame) to GCS under experiment path.

        Returns: GCS URI of saved artifact
        """
        gcs_path = f"{EXPERIMENT_CONFIG.experiments_path}{self.experiment_name}/{self.run_name}/{filename}"

        if STORAGE_CLIENT:
            uri = upload_df_to_gcs(data, gcs_path)
        else:
            # Local fallback
            local_path = os.path.join(CONFIG.LOCAL_DATA_DIR, 'experiments',
                                     self.experiment_name, self.run_name, filename)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            data.to_csv(local_path, index=False)
            uri = local_path
            print(f"✓ Saved locally: {local_path}")

        self.artifacts.append({
            'filename': filename,
            'uri': uri,
            'type': artifact_type,
            'rows': len(data)
        })

        return uri

    def log_model(self, model, model_name: str) -> str:
        """
        Save trained model to GCS.

        Args:
            model: XGBoost or sklearn model
            model_name: Name for the model file

        Returns: GCS URI of saved model
        """
        import joblib
        import tempfile

        gcs_path = f"{EXPERIMENT_CONFIG.experiments_path}{self.experiment_name}/{self.run_name}/models/{model_name}.joblib"

        if STORAGE_CLIENT:
            # Save to temp file, then upload
            with tempfile.NamedTemporaryFile(suffix='.joblib', delete=False) as f:
                joblib.dump(model, f.name)

                bucket = STORAGE_CLIENT.bucket(CONFIG.GCS_BUCKET)
                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(f.name)

                os.unlink(f.name)

            uri = f"gs://{CONFIG.GCS_BUCKET}/{gcs_path}"
            print(f"✓ Model saved to GCS: {uri}")
        else:
            local_path = os.path.join(CONFIG.LOCAL_DATA_DIR, 'experiments',
                                     self.experiment_name, self.run_name, 'models', f"{model_name}.joblib")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            joblib.dump(model, local_path)
            uri = local_path
            print(f"✓ Model saved locally: {local_path}")

        self.artifacts.append({
            'filename': f"{model_name}.joblib",
            'uri': uri,
            'type': 'model'
        })

        return uri

    def end_run(self, status: str = 'completed'):
        """End current run and save summary."""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()

        # Create run summary
        summary = {
            'experiment_name': self.experiment_name,
            'run_name': self.run_name,
            'start_time': self.start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration,
            'status': status,
            'params': self.params,
            'metrics': self.metrics,
            'artifacts': self.artifacts
        }

        # Save summary
        summary_path = f"{EXPERIMENT_CONFIG.experiments_path}{self.experiment_name}/{self.run_name}/run_summary.json"

        if STORAGE_CLIENT:
            bucket = STORAGE_CLIENT.bucket(CONFIG.GCS_BUCKET)
            blob = bucket.blob(summary_path)
            blob.upload_from_string(json.dumps(summary, indent=2), content_type='application/json')
            print(f"✓ Run summary saved to GCS")
        else:
            local_path = os.path.join(CONFIG.LOCAL_DATA_DIR, 'experiments',
                                     self.experiment_name, self.run_name, 'run_summary.json')
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, 'w') as f:
                json.dump(summary, f, indent=2)

        if self.use_vertex:
            try:
                aiplatform.end_run()
            except Exception as e:
                print(f"Failed to end Vertex run: {e}")

        print(f"\n{'='*60}")
        print(f"RUN COMPLETED: {self.run_name}")
        print(f"{'='*60}")
        print(f"Duration: {duration:.1f}s")
        print(f"Params: {len(self.params)}")
        print(f"Metrics: {self.metrics}")
        print(f"Artifacts: {len(self.artifacts)}")
        print(f"{'='*60}\n")

        return summary

    @staticmethod
    def list_experiments() -> List[Dict]:
        """List all experiments."""
        if VERTEX_AVAILABLE:
            try:
                experiments = aiplatform.Experiment.list()
                return [{'name': e.name, 'description': e.description} for e in experiments]
            except:
                pass

        # Fallback: list from GCS
        if STORAGE_CLIENT:
            bucket = STORAGE_CLIENT.bucket(CONFIG.GCS_BUCKET)
            blobs = bucket.list_blobs(prefix=EXPERIMENT_CONFIG.experiments_path, delimiter='/')

            experiments = []
            for prefix in blobs.prefixes:
                exp_name = prefix.replace(EXPERIMENT_CONFIG.experiments_path, '').rstrip('/')
                experiments.append({'name': exp_name})
            return experiments

        return []

    @staticmethod
    def get_experiment_runs(experiment_name: str) -> List[Dict]:
        """Get all runs for an experiment."""
        runs = []

        if STORAGE_CLIENT:
            bucket = STORAGE_CLIENT.bucket(CONFIG.GCS_BUCKET)
            prefix = f"{EXPERIMENT_CONFIG.experiments_path}{experiment_name}/"
            blobs = bucket.list_blobs(prefix=prefix, delimiter='/')

            for run_prefix in blobs.prefixes:
                run_name = run_prefix.replace(prefix, '').rstrip('/')

                # Try to load summary
                summary_blob = bucket.blob(f"{run_prefix}run_summary.json")
                try:
                    summary = json.loads(summary_blob.download_as_string())
                    runs.append(summary)
                except:
                    runs.append({'run_name': run_name})

        return runs

    @staticmethod
    def compare_runs(experiment_name: str, metric_keys: List[str] = None) -> pd.DataFrame:
        """
        Compare all runs in an experiment.

        Returns DataFrame with params and metrics for each run.
        """
        runs = ExperimentTracker.get_experiment_runs(experiment_name)

        if not runs:
            print(f"No runs found for experiment: {experiment_name}")
            return pd.DataFrame()

        # Flatten runs into rows
        rows = []
        for run in runs:
            row = {
                'run_name': run.get('run_name', 'unknown'),
                'status': run.get('status', 'unknown'),
                'duration_seconds': run.get('duration_seconds', 0)
            }

            # Add params
            for k, v in run.get('params', {}).items():
                row[f'param_{k}'] = v

            # Add metrics
            for k, v in run.get('metrics', {}).items():
                row[f'metric_{k}'] = v

            rows.append(row)

        df = pd.DataFrame(rows)

        # Filter metrics if specified
        if metric_keys:
            metric_cols = [f'metric_{k}' for k in metric_keys if f'metric_{k}' in df.columns]
            keep_cols = ['run_name', 'status', 'duration_seconds'] + \
                       [c for c in df.columns if c.startswith('param_')] + metric_cols
            df = df[[c for c in keep_cols if c in df.columns]]

        return df.sort_values('run_name')


def create_experiment(
    name: str,
    description: str = None,
    model_version: str = None,
    exclude_weeks: List[int] = None,
    **kwargs
) -> ExperimentTracker:
    """
    Convenience function to create a new experiment with standard naming.

    Args:
        name: Base experiment name (e.g., 'baseline', 'no_w47', 'price_features')
        description: Experiment description
        model_version: V1, V2, V3, V4, etc.
        exclude_weeks: Weeks to exclude (e.g., [47] for Black Friday)
        **kwargs: Additional parameters to log

    Returns:
        ExperimentTracker instance
    """
    # Create experiment name with version
    exp_name = name
    if model_version:
        exp_name = f"{model_version}_{name}"

    # Add timestamp for uniqueness
    timestamp = datetime.now().strftime('%Y%m%d')
    exp_name = f"{exp_name}_{timestamp}"

    tracker = ExperimentTracker(exp_name, description)

    # Log standard params
    params = {
        'model_version': model_version or 'unknown',
        'exclude_weeks': str(exclude_weeks) if exclude_weeks else 'none',
        'created_at': datetime.now().isoformat(),
        **kwargs
    }

    return tracker


# Convenience functions for common experiment types
def baseline_experiment(model_version: str = 'V2') -> ExperimentTracker:
    """Create baseline experiment."""
    return create_experiment(
        name='baseline',
        description=f'Baseline {model_version} model with default settings',
        model_version=model_version,
        min_h1_weeks=CONFIG.MIN_H1_WEEKS,
        high_conf_weeks=CONFIG.HIGH_CONF_WEEKS
    )


def no_blackfriday_experiment(model_version: str = 'V5') -> ExperimentTracker:
    """Create experiment excluding Black Friday (W47)."""
    return create_experiment(
        name='no_w47',
        description=f'{model_version} model excluding Black Friday week',
        model_version=model_version,
        exclude_weeks=[47],
        min_h1_weeks=CONFIG.MIN_H1_WEEKS
    )


def custom_experiment(name: str, **params) -> ExperimentTracker:
    """Create custom experiment with any parameters."""
    return create_experiment(name=name, **params)
