"""
Iris Classification Flow using MLflow and Prefect.
This is a boilerplate that needs to be completed as part of the challenge.
"""

import os
from typing import Tuple
import numpy as np
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
import mlflow
from prefect import flow, task, get_run_logger
from prefect.variables import Variable

def setup_variables():
    """Initialize Prefect variables if they don't exist."""
    # TODO: Create the following variables using Variable.set():
    # - rf_n_estimators: number of trees (default: 100)
    # - rf_max_depth: maximum depth of trees (default: 10)
    # - rf_test_size: test split ratio (default: 0.2)
    # - model_name: name for MLflow logging (default: iris_model)
    # - model_version: version for MLflow logging (default: 1)
    Variable.set("rf_n_estimators", 100)
    Variable.set("rf_max_depth", 10)
    Variable.set("rf_test_size", 0.2)
    Variable.set("model_name", "iris_model")
    Variable.set("model_version", 1)
    

@task
def load_data() -> Tuple[np.ndarray, np.ndarray]:
    """Load the iris dataset."""
    # TODO: Implement data loading
    # 1. Get a logger using get_run_logger()
    # 2. Load iris dataset using load_iris(return_X_y=True)
    # 3. Log the dataset shape
    # 4. Return X, y
    logger = get_run_logger()
    X, y = load_iris(return_X_y=True)
    logger.info(f"Dataset loaded with shape: {X.shape}")
    return X, y

@task
def split_data(X: np.ndarray, y: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Split data into train and test sets."""
    # TODO: Implement data splitting
    # 1. Get test_size from Prefect variables
    # 2. Use train_test_split
    # 3. Return X_train, X_test, y_train, y_test
    test_size = float(Variable.get("rf_test_size"))
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42, stratify=y)
    return X_train, X_test, y_train, y_test

@task
def train_model(X_train: np.ndarray, y_train: np.ndarray) -> RandomForestClassifier:
    """Train model using varia@bles for hyperparameters."""
    # TODO: Implement model training
    # 1. Get hyperparameters from Prefect variables (n_estimators, max_depth)
    # 2. Create and train RandomForestClassifier
    # 3. Return the trained model
    n_estimators = int(Variable.get("rf_n_estimators"))
    max_depth = int(Variable.get("rf_max_depth"))
    model = RandomForestClassifier(n_estimators=n_estimators, max_depth=max_depth, random_state=42)
    model.fit(X_train, y_train)
    return model

@task
def evaluate_model(model: RandomForestClassifier, X_test: np.ndarray, y_test: np.ndarray) -> float:
    """Evaluate the model and log metrics."""
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("n_test_samples", len(y_test))
    return accuracy

@flow
def iris_classification_flow() -> float:
    """Main flow for iris classification."""
    # TODO: Implement the main flow
    # 1. Configure MLflow tracking URI from environment variable
    # 2. Set MLflow experiment name
    # 3. Get model_name and version from variables
    # 4. Execute tasks in order:
    #    - load_data()
    #    - split_data()
    #    - train_model()
    #    - evaluate_model()
    # 5. Log the model using mlflow.sklearn.log_model()
    # 6. Return the accuracy
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(tracking_uri)
    experiment_name = "iris_classification"
    mlflow.set_experiment(experiment_name)
    model_name = Variable.get("model_name")
    model_version = Variable.get("model_version")
    
    n_estimators = int(Variable.get("rf_n_estimators"))
    max_depth = int(Variable.get("rf_max_depth"))
    test_size = float(Variable.get("rf_test_size"))
    
    with mlflow.start_run(run_name="iris_classification_run", nested=True):
        X, y = load_data()
        X_train, X_test, y_train, y_test = split_data(X, y)
        model = train_model(X_train, y_train)
        accuracy = evaluate_model(model, X_test, y_test)
        mlflow.sklearn.log_model(sk_model=model, name=model_name, registered_model_name=model_name)
        
        mlflow.log_params({
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "test_size": test_size,
        })
        
        mlflow.log_metrics({
        "accuracy": accuracy,
        "n_test_samples": len(y_test),
        })
        
        feature_names = [
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
        ]

        for feature_name, importance in zip(feature_names, model.feature_importances_):
            mlflow.log_metric(
                f"importance_{feature_name}",
                float(importance),
            )
        return accuracy

if __name__ == "__main__":
    setup_variables()
    iris_classification_flow()