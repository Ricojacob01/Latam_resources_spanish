# demo_setup/experiment_manager.py

import mlflow
from mlflow import MlflowClient


class ExperimentManager:
    """
    Manages MLflow experiments and tracking configuration.

    Responsible for:
    - Setting the MLflow tracking URI
    - Creating, deleting, and activating experiments
    - Standalone MLflow setup (tracking URI + experiment) independent of agent registration
    """

    def __init__(self, username: str, tracking_uri: str = "databricks"):
        """
        Initialize experiment manager.

        Parameters
        ----------
        username : str
            Databricks username for experiment path
        tracking_uri : str
            MLflow tracking URI (default: "databricks")
        """
        self.username = username
        self.tracking_uri = tracking_uri
        mlflow.set_tracking_uri(self.tracking_uri)

    def create_experiment(self, experiment_name: str) -> str:
        """
        Create or reuse an experiment, restoring it if it was soft-deleted.

        Parameters
        ----------
        experiment_name : str
            Name of the experiment

        Returns
        -------
        str
            Full experiment path
        """
        print(f"Setting up experiment: {experiment_name}")
        experiment_path = f"/Workspace/Users/{self.username}/{experiment_name}"

        exp = mlflow.get_experiment_by_name(experiment_path)
        if exp is not None:
            if exp.lifecycle_stage == "deleted":
                print(f"  Restoring deleted experiment: {experiment_name}")
                mfc = MlflowClient()
                mfc.restore_experiment(exp.experiment_id)
                print(f"  Restored experiment {exp.experiment_id}")
            else:
                print(f"  Experiment already exists: {experiment_name}")
        else:
            mlflow.create_experiment(experiment_path)
            print(f"  Created experiment: {experiment_path}")

        return experiment_path

    def set_experiment(self, experiment_path: str) -> None:
        """Set the active MLflow experiment."""
        mlflow.set_tracking_uri(self.tracking_uri)
        mlflow.set_experiment(experiment_path)

    def setup(self, experiment_name: str) -> str:
        """
        Standalone MLflow setup: set tracking URI, create experiment, and activate it.

        This can be called independently of agent registration when a course needs
        MLflow configured (e.g., for tracing or manual evaluation runs) without
        going through the full agent setup pipeline.

        Parameters
        ----------
        experiment_name : str
            Name of the experiment to create and activate

        Returns
        -------
        str
            Full experiment path
        """
        print(f"  Setting tracking URI: {self.tracking_uri}")
        mlflow.set_tracking_uri(self.tracking_uri)
        experiment_path = self.create_experiment(experiment_name)
        self.set_experiment(experiment_path)
        print(f"  Active experiment: {experiment_path}")
        return experiment_path