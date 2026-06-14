"""
Real-time anomaly detection using Isolation Forest on streaming data.
Integrates with MLflow for model tracking and Databricks for serving.
"""
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import mlflow
import mlflow.sklearn
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, current_timestamp
from pyspark.sql.types import DoubleType, StringType

class StreamingAnomalyDetector:
    def __init__(self, model_name: str = "anomaly_detection_model", model_version: int = 1):
        self.model_name = model_name
        self.model_version = model_version
        self.model = None
        self.scaler = StandardScaler()
        self.mlflow_uri = "databricks"
        
    def load_model(self):
        """Load model from MLflow Model Registry."""
        mlflow.set_tracking_uri(self.mlflow_uri)
        model_uri = f"models:/{self.model_name}/{self.model_version}"
        self.model = mlflow.sklearn.load_model(model_uri)
        print(f"Loaded model {self.model_name} version {self.model_version}")
    
    def train_model(self, historical_data_path: str, contamination: float = 0.05):
        """Train Isolation Forest on historical data and register to MLflow."""
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.parquet(historical_data_path).toPandas()
        
        features = df[['value', 'rolling_mean', 'rolling_std', 'hour_of_day']].fillna(0)
        X = self.scaler.fit_transform(features)
        
        model = IsolationForest(
            contamination=contamination,
            random_state=42,
            n_estimators=100
        )
        model.fit(X)
        
        with mlflow.start_run(run_name="anomaly_model_training") as run:
            mlflow.log_param("contamination", contamination)
            mlflow.log_param("n_estimators", 100)
            mlflow.sklearn.log_model(model, "model", registered_model_name=self.model_name)
            mlflow.log_artifact("src/ml/anomaly_detection.py")
            run_id = run.info.run_id
        
        print(f"Model trained and registered. Run ID: {run_id}")
        return model
    
    def detect_anomalies(self, spark_df, feature_cols):
        """Apply anomaly detection to streaming DataFrame."""
        if self.model is None:
            self.load_model()
        
        @udf(returnType=DoubleType())
        def predict_anomaly(*features):
            X = self.scaler.transform([features])
            score = self.model.decision_function(X)[0]
            return float(score)
        
        @udf(returnType=StringType())
        def classify_anomaly(score):
            if score < -0.3:
                return "CRITICAL"
            elif score < -0.1:
                return "WARNING"
            else:
                return "NORMAL"
        
        return (
            spark_df
            .withColumn("anomaly_score", predict_anomaly(*[col(c) for c in feature_cols]))
            .withColumn("anomaly_class", classify_anomaly(col("anomaly_score")))
            .withColumn("detection_timestamp", current_timestamp())
        )

def main():
    detector = StreamingAnomalyDetector()
    
    spark = SparkSession.builder.appName("AnomalyDetection").getOrCreate()
    streaming_df = spark.readStream.format("delta").load("dbfs:/mnt/streaming/silver/events")
    
    feature_cols = ["value", "rolling_mean", "rolling_std", "hour_of_day"]
    anomalies_df = detector.detect_anomalies(streaming_df, feature_cols)
    
    query = (
        anomalies_df
        .filter(col("anomaly_class").isin(["CRITICAL", "WARNING"]))
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/dbfs/mnt/checkpoints/anomalies")
        .trigger(processingTime="15 seconds")
        .start("dbfs:/mnt/streaming/gold/anomalies")
    )
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
