# ================================================================
# Script : Transformation Bronze → Silver
# ================================================================

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import *

# Configuration
BRONZE_PATH = 'D:/lakehouse/bronze'
SILVER_PATH = 'D:/lakehouse/silver'

# Créer session Spark
builder = SparkSession.builder.appName("Silver Transformation").master("local[*]")\
                                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("="*70)
print("TRANSFORMATION SILVER")
print("="*70)

# Lire Bronze
df_clients = spark.read.format("delta").load(f"{BRONZE_PATH}/clients")

# Lire Bronze ventes
df_ventes = spark.read.format("delta").load(f"{BRONZE_PATH}/ventes")



# Nettoyage :
# 1. Supprimer les doublons
# 2. Standardiser les noms (majuscules)
# 3. Valider les emails
df_clients_clean = df_clients \
    .dropDuplicates(['client_id']) \
    .withColumn('nom', upper(col('nom'))) \
    .withColumn('prenom', initcap(col('prenom'))) \
    .filter(col('email').rlike('^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'))


print(f"Clients Bronze : {df_clients.count()}")
print(f"Clients Silver (après nettoyage) : {df_clients_clean.count()}")

# EXPLICATIONS :
# - dropDuplicates(['client_id']) : supprime les doublons sur client_id
# - upper(col('nom')) : met le nom en MAJUSCULES
# - initcap(col('prenom')) : Met La Première Lettre En Majuscule
# - rlike(...) : filtre avec une regex pour valider le format email


# Nettoyage simple pour ventes :
# 1. Supprimer doublons (si vente_id unique)
# 2. Filtrer quantités positives
# 3. Ajouter colonne 'mois_vente' pour agrégations futures
df_ventes_clean = df_ventes \
    .dropDuplicates(['vente_id']) \
    .filter(col('quantite') > 0) \
    .withColumn('mois_vente', date_format(col('date_vente'), 'yyyy-MM'))


print(f"Ventes Bronze : {df_ventes.count()}")
print(f"Ventes Silver (après nettoyage) : {df_ventes_clean.count()}")


# Écrire en Silver
df_clients_clean.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/clients")
df_ventes_clean.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/ventes")

print("✓ Silver clients créé")
print("✓ Silver ventes créé")

spark.stop()