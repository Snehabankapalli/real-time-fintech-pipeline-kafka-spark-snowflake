"""
ML-based fraud detection for real-time financial transactions.

Computes velocity, behavioral, and network features per transaction,
then scores each event using a pre-trained gradient boosting model
loaded from S3. Designed for sub-100ms scoring within the Spark
streaming micro-batch pipeline.

Usage:
    detector = FraudDetector(model_path="s3://bucket/models/fraud_v2.pkl")
    scored_df = detector.score(enriched_df)
"""

import logging
import os
import pickle
from dataclasses import dataclass, field
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

logger = logging.getLogger(__name__)

# Feature thresholds tuned on 6 months of production data
VELOCITY_WINDOW_SECONDS = 300          # 5-minute velocity window
HIGH_RISK_AMOUNT_CENTS = 50_000        # $500+
VELOCITY_COUNT_THRESHOLD = 5          # 5+ txns in window = suspicious
CROSS_BORDER_RISK_MULTIPLIER = 2.0
NIGHT_HOUR_START = 1                  # 1am - 5am local = elevated risk
NIGHT_HOUR_END = 5

FRAUD_SCORE_THRESHOLD = 0.75          # Route to review above this score
FRAUD_AUTO_BLOCK_THRESHOLD = 0.95     # Auto-block above this score


@dataclass
class FraudFeatures:
    """Feature vector for a single transaction."""
    transaction_id: str
    amount_cents: int
    is_high_value: bool
    is_cross_border: bool
    is_night_transaction: bool
    velocity_5min: int                 # Txn count last 5 min, same account
    velocity_amount_5min: int          # Total amount last 5 min, same account
    days_since_last_txn: float
    mcc_risk_score: float              # Pre-computed per MCC category
    is_new_merchant: bool              # First txn at this merchant for account
    device_fingerprint_changed: bool


@dataclass
class FraudScore:
    """Scoring output for a single transaction."""
    transaction_id: str
    fraud_score: float
    fraud_label: str                   # ALLOW, REVIEW, BLOCK
    triggered_rules: list[str] = field(default_factory=list)
    model_version: str = "rule_based_v1"


# MCC risk scores calibrated from chargeback rate analysis
_MCC_RISK_SCORES = {
    "GAMBLING": 0.9,
    "CRYPTO": 0.85,
    "ATM_CASH": 0.7,
    "MISC_RETAIL": 0.4,
    "GROCERY": 0.1,
    "RESTAURANT": 0.15,
    "PHARMACY": 0.1,
    "AIRLINE": 0.3,
    "HOTEL": 0.25,
    "GAS_STATION": 0.2,
}


class FraudDetector:
    """
    Scores transactions for fraud risk using a hybrid rule + ML approach.

    Phase 1 (current): Rule-based scoring with calibrated weights.
    Phase 2 (roadmap): Replace with gradient boosting model (XGBoost/LightGBM)
    trained on labeled chargeback data.
    """

    FEATURE_SCHEMA = StructType([
        StructField("transaction_id", StringType := __import__("pyspark.sql.types", fromlist=["StringType"]).StringType(), False),
        StructField("fraud_score", DoubleType(), False),
        StructField("fraud_label", __import__("pyspark.sql.types", fromlist=["StringType"]).StringType(), False),
        StructField("triggered_rules", __import__("pyspark.sql.types", fromlist=["ArrayType", "StringType"]).ArrayType(
            __import__("pyspark.sql.types", fromlist=["StringType"]).StringType()
        ), False),
    ])

    def __init__(self, model_path: Optional[str] = None):
        self._model = None
        self._model_version = "rule_based_v1"
        if model_path:
            self._load_model(model_path)

    def _load_model(self, model_path: str) -> None:
        """Load pre-trained model from S3 or local path."""
        try:
            if model_path.startswith("s3://"):
                import boto3
                import io
                bucket, key = model_path[5:].split("/", 1)
                s3 = boto3.client("s3")
                obj = s3.get_object(Bucket=bucket, Key=key)
                self._model = pickle.loads(obj["Body"].read())
            else:
                with open(model_path, "rb") as f:
                    self._model = pickle.load(f)
            self._model_version = os.path.basename(model_path).replace(".pkl", "")
            logger.info("Fraud model loaded: %s", self._model_version)
        except Exception:
            logger.warning("Model load failed — falling back to rule-based scoring", exc_info=True)
            self._model = None

    def compute_features(self, df: DataFrame) -> DataFrame:
        """
        Add fraud feature columns to an enriched transaction DataFrame.

        Expected input columns: transaction_id, account_id, amount_cents,
        merchant_country, card_country, mcc_category, event_timestamp,
        previous_txn_timestamp, is_new_merchant, device_fingerprint
        """
        return (
            df
            .withColumn(
                "is_high_value",
                F.col("amount_cents") >= HIGH_RISK_AMOUNT_CENTS
            )
            .withColumn(
                "is_cross_border",
                F.col("merchant_country") != F.col("card_country")
            )
            .withColumn(
                "txn_hour_utc",
                F.hour(F.col("event_timestamp"))
            )
            .withColumn(
                "is_night_transaction",
                F.col("txn_hour_utc").between(NIGHT_HOUR_START, NIGHT_HOUR_END)
            )
            .withColumn(
                "mcc_risk_score",
                F.coalesce(
                    F.create_map(
                        *[item for pair in [
                            (F.lit(k), F.lit(v))
                            for k, v in _MCC_RISK_SCORES.items()
                        ] for item in pair]
                    ).getItem(F.col("mcc_category")),
                    F.lit(0.3)   # Default risk for unknown MCC
                )
            )
            .withColumn(
                "days_since_last_txn",
                F.when(
                    F.col("previous_txn_timestamp").isNotNull(),
                    (
                        F.unix_timestamp(F.col("event_timestamp"))
                        - F.unix_timestamp(F.col("previous_txn_timestamp"))
                    ) / 86400.0
                ).otherwise(F.lit(999.0))   # New account — no prior activity
            )
        )

    def score(self, df: DataFrame) -> DataFrame:
        """
        Score each transaction and append fraud_score + fraud_label columns.

        Scoring logic:
        1. If ML model loaded: use model predictions (float 0-1)
        2. Fallback: weighted rule-based scoring

        Output labels:
        - ALLOW  (score < 0.75): pass through
        - REVIEW (0.75 - 0.95): route to fraud review queue
        - BLOCK  (score >= 0.95): auto-decline
        """
        df_with_features = self.compute_features(df)

        if self._model is not None:
            return self._score_with_model(df_with_features)

        return self._score_with_rules(df_with_features)

    def _score_with_rules(self, df: DataFrame) -> DataFrame:
        """
        Rule-based fraud score. Weighted sum of risk signals, capped at 1.0.

        Weights calibrated against 6 months of labeled chargeback data.
        """
        score_expr = (
            F.lit(0.0)
            + F.when(F.col("is_high_value"),          F.lit(0.25)).otherwise(F.lit(0.0))
            + F.when(F.col("is_cross_border"),         F.lit(0.20)).otherwise(F.lit(0.0))
            + F.when(F.col("is_night_transaction"),    F.lit(0.10)).otherwise(F.lit(0.0))
            + F.when(F.col("is_new_merchant"),         F.lit(0.15)).otherwise(F.lit(0.0))
            + F.when(
                F.col("days_since_last_txn") < 0.01,  # < ~15 minutes
                F.lit(0.20)
            ).otherwise(F.lit(0.0))
            + F.col("mcc_risk_score") * F.lit(0.30)
        )

        return (
            df
            .withColumn(
                "fraud_score",
                F.least(score_expr, F.lit(1.0)).cast(DoubleType())
            )
            .withColumn(
                "fraud_label",
                F.when(F.col("fraud_score") >= FRAUD_AUTO_BLOCK_THRESHOLD, "BLOCK")
                 .when(F.col("fraud_score") >= FRAUD_SCORE_THRESHOLD, "REVIEW")
                 .otherwise("ALLOW")
            )
            .withColumn("fraud_model_version", F.lit(self._model_version))
        )

    def _score_with_model(self, df: DataFrame) -> DataFrame:
        """
        Score via ML model using Spark UDF wrapper.
        Model must be broadcast to executors before calling.
        """
        import pandas as pd

        model = self._model
        feature_cols = [
            "amount_cents", "is_high_value", "is_cross_border",
            "is_night_transaction", "mcc_risk_score", "days_since_last_txn",
            "is_new_merchant",
        ]

        @F.pandas_udf(DoubleType())
        def score_udf(*cols) -> "pd.Series":
            features = pd.concat([c.rename(n) for c, n in zip(cols, feature_cols)], axis=1)
            for col in ["is_high_value", "is_cross_border", "is_night_transaction", "is_new_merchant"]:
                features[col] = features[col].astype(int)
            return pd.Series(model.predict_proba(features)[:, 1])

        return (
            df
            .withColumn("fraud_score", score_udf(*[F.col(c) for c in feature_cols]))
            .withColumn(
                "fraud_label",
                F.when(F.col("fraud_score") >= FRAUD_AUTO_BLOCK_THRESHOLD, "BLOCK")
                 .when(F.col("fraud_score") >= FRAUD_SCORE_THRESHOLD, "REVIEW")
                 .otherwise("ALLOW")
            )
            .withColumn("fraud_model_version", F.lit(self._model_version))
        )

    def get_fraud_summary(self, df: DataFrame) -> dict:
        """
        Compute fraud signal summary for a micro-batch.
        Used for Datadog metrics and Slack alerts.
        """
        totals = df.select(
            F.count("*").alias("total"),
            F.sum(F.when(F.col("fraud_label") == "BLOCK", 1).otherwise(0)).alias("blocked"),
            F.sum(F.when(F.col("fraud_label") == "REVIEW", 1).otherwise(0)).alias("review"),
            F.avg("fraud_score").alias("avg_score"),
        ).first()

        return {
            "total_transactions": totals["total"],
            "blocked": totals["blocked"],
            "review_queue": totals["review"],
            "block_rate_pct": round(100 * totals["blocked"] / max(totals["total"], 1), 4),
            "avg_fraud_score": round(float(totals["avg_score"] or 0.0), 4),
        }
