from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import uuid

def flatten_dict(d, parent_key='', sep='_'):
    """Aplana un diccionario anidado en un solo nivel"""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def explode_column(df: DataFrame, col_name: str, new_col_name: str):
    """Explota listas en un DataFrame y mantiene referencia"""
    return df.withColumn(new_col_name, F.explode_outer(F.col(col_name)))
