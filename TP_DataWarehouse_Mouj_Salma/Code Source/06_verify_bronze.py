from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Créer session Spark simple
builder = SparkSession.builder.appName("Verify Bronze").master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Lire une table Delta
df = spark.read.format("delta").load("D:/lakehouse/bronze/clients")

print("Données Bronze clients :")
df.show()

print("\nSchéma :")
df.printSchema()

print(f"\nNombre de lignes : {df.count()}")

spark.stop()