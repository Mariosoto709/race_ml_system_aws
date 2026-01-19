from awsglue.dynamicframe import DynamicFrame
from flatten_helpers import flatten_dict, explode_column
import uuid
import pyspark.sql.functions as F

CHUNK_SIZE = 10  # Ajusta según tamaño de eventos por fila (menos = más seguro)

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

    def transform(self):
        df = self.dyf.toDF()

        # Explode la columna "events" si existe
        if "events" in df.columns:
            df_exploded = explode_column(df, "events", "events_list")
        else:
            df_exploded = df

        def flatten_event(row):
            base = {k: v for k, v in row.asDict().items() if k != "events_list"}
            events = getattr(row, "events_list", [])
            if not isinstance(events, list):
                events = [events]
            flat_rows = []
            # Dividir los eventos en chunks para no superar 64MB por fila
            for i in range(0, len(events), CHUNK_SIZE):
                chunk = events[i:i+CHUNK_SIZE]
                for event in chunk:
                    event_flat = flatten_dict(event)
                    row_flat = base.copy()
                    row_flat.update(event_flat)
                    row_flat["athlete_id"] = str(base.get("id", uuid.uuid4()))
                    row_flat["event_id"] = str(event_flat.get("id", uuid.uuid4()))
                    flat_rows.append(row_flat)
            return flat_rows

        rdd_flat = df_exploded.rdd.flatMap(flatten_event)

        # Convertimos a DynamicFrame
        self.dyf_flat = DynamicFrame.fromDF(rdd_flat.toDF(), self.glueContext, f"{self.entity_name}_dyf")

    def load(self):
        self.glueContext.write_dynamic_frame.from_options(
            frame=self.dyf_flat,
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

    def transform(self):
        df = self.dyf.toDF()

        def flatten_issue_row(row):
            d = row.asDict()
            issue_id = d.get("id")
            athlete_data = d.get("data", {})
            athlete_id = d.get("athlete_id") or athlete_data.get("id")
            event_id = d.get("event", "independiente")

            rows = []

            # Registro base
            base = {k: v for k, v in d.items() if k != "data"}
            base.update({"issue_id": issue_id, "athlete_id": athlete_id, "event_id": event_id})
            rows.append(base)

            # Datos aplanados
            data_flat = {f"data_{k}": v for k, v in athlete_data.items() if k != "events"}
            data_flat.update({"issue_id": issue_id, "athlete_id": athlete_id, "event_id": event_id})
            rows.append(data_flat)

            # Eventos del atleta
            events = athlete_data.get("events", [])
            for i in range(0, len(events), CHUNK_SIZE):
                chunk = events[i:i+CHUNK_SIZE]
                for idx, event_val in enumerate(chunk):
                    event_flat = flatten_dict(event_val)
                    event_flat.update({
                        "issue_id": issue_id,
                        "athlete_id": athlete_id,
                        "event_index": idx,
                        "event_id": event_id
                    })
                    rows.append(event_flat)

            return rows

        rdd_flat = df.rdd.flatMap(flatten_issue_row)

        # Convertimos a DynamicFrame
        self.dyf_flat = DynamicFrame.fromDF(rdd_flat.toDF(), self.glueContext, "issues_dyf")

    def load(self):
        self.glueContext.write_dynamic_frame.from_options(
            frame=self.dyf_flat,
            connection_type="s3",
            connection_options={"path": self.output_path},
            format="parquet"
        )
        print(f"Issues procesados y guardados en {self.output_path}")

