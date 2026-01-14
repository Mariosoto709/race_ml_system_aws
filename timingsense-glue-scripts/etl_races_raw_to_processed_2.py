from awsglue.context import GlueContext
from pyspark.context import SparkContext
from races_etl_classes import RacesETL, IssuesETL

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

# Rutas S3
athletes_input = "s3://timingsense-races-raw/athletes/"
athletes_output = "s3://timingsense-races-processed/athletes/"

issues_input = "s3://timingsense-races-raw/issues/"
issues_output = "s3://timingsense-races-processed/issues/"

# Ejecutamos ETL
athletes_job = RacesETL(glueContext, athletes_input, athletes_output, "athletes")
athletes_job.extract()
athletes_job.transform()
athletes_job.load()

issues_job = IssuesETL(glueContext, issues_input, issues_output)
issues_job.extract()
issues_job.transform()
issues_job.load()