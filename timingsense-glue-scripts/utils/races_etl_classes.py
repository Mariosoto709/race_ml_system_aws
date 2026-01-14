from awsglue.dynamicframe import DynamicFrame
from flatten_helpers import flatten_dict, explode_column
from pyspark.sql import DataFrame, SparkSession
import uuid

spark = SparkSession.builder.getOrCreate()

class RacesETL:
    def __init__(self, glueContext, input_path, output_path, entity_name):
        self.glueContext = glueContext
        self.input_path = input_path
        self.output_path = output_path
        self.entity_name = entity_name

    def extract(self):
        self.dyf = self.glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [self.input_path]},
            format="json"
        )
        self.df = self.dyf.toDF()

    def transform(self):
        self.df = self.df.withColumn("athlete_id", F.col("id").cast("string"))
        if "events" in self.df.columns:
            self.df_events = explode_column(self.df, "events", "event")
        else:
            self.df_events = self.df

        def flatten_event(row):
            base = flatten_dict({k: v for k, v in row.asDict().items() if k != "event"})
            event = row.event if row.event else {}
            event_flat = flatten_dict(event)
            base.update(event_flat)
            base["event_id"] = str(event_flat.get("id", str(uuid.uuid4())))
            return base

        rdd_flat = self.df_events.rdd.map(flatten_event)
        self.df_clean = spark.createDataFrame(rdd_flat)
        for col in ["athlete_id", "event_id"]:
            if col in self.df_clean.columns:
                self.df_clean = self.df_clean.withColumn(col, F.col(col).cast("string"))

    def load(self):
        dyf_out = DynamicFrame.fromDF(self.df_clean, self.glueContext, f"{self.entity_name}_dyf")
        self.glueContext.write_dynamic_frame.from_options(
            frame=dyf_out,
            connection_type="s3",
            connection_options={"path": self.output_path},
            format="parquet"
        )
        print(f"{self.entity_name} procesado y guardado en {self.output_path}")


class IssuesETL:
    def __init__(self, glueContext, input_path, output_path):
        self.glueContext = glueContext
        self.input_path = input_path
        self.output_path = output_path

    def extract(self):
        self.dyf = self.glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [self.input_path]},
            format="json"
        )
        self.df = self.dyf.toDF()

    def transform(self):
        rdd_flat = self.df.rdd.flatMap(self.flatten_issue)
        self.df_clean = spark.createDataFrame(rdd_flat)
        for col in ["athlete_id", "event_id", "issue_id"]:
            if col in self.df_clean.columns:
                self.df_clean = self.df_clean.withColumn(col, F.col(col).cast("string"))

    def flatten_issue(self, row):
        flat_rows = []
        d = row.asDict()
        issue_id = d.get("id")
        athlete_data = d.get("data", {})
        athlete_id = d.get("athlete_id") or athlete_data.get("id")
        event_id = d.get("event", "independiente")

        base = {k: v for k, v in d.items() if k != "data"}
        base.update({"issue_id": issue_id, "athlete_id": athlete_id, "event_id": event_id})
        flat_rows.append(base)

        data_flat = {f"data_{k}": v for k, v in athlete_data.items() if k != "events"}
        data_flat.update({"issue_id": issue_id, "athlete_id": athlete_id, "event_id": event_id})
        flat_rows.append(data_flat)

        for idx, event_val in enumerate(athlete_data.get("events", [])):
            event_flat = flatten_dict(event_val)
            event_flat.update({
                "issue_id": issue_id,
                "athlete_id": athlete_id,
                "event_index": idx,
                "event_id": event_id
            })
            flat_rows.append(event_flat)

        return flat_rows

    def load(self):
        dyf_out = DynamicFrame.fromDF(self.df_clean, self.glueContext, "issues_dyf")
        self.glueContext.write_dynamic_frame.from_options(
            frame=dyf_out,
            connection_type="s3",
            connection_options={"path": self.output_path},
            format="parquet"
        )
        print(f"Issues procesados y guardados en {self.output_path}")
