# Especificar interprete de Python para los workers y el driver de Spark
import os
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, asc, udf, desc, coalesce, lit, avg, count, max, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import reverse_geocoder as rg
import pycountry

# Inicializar sesión Spark
spark = SparkSession.builder \
    .appName("NASA Meteorites Analysis") \
    .getOrCreate()

# Definir esquema
schema = StructType([
    StructField("name", StringType(), True),          # Nombre, suele coincidir con el lugar de caída
    StructField("id", StringType(), True),            # Id
    StructField("nametype", StringType(), True),      # Tipo de nombre
    StructField("recclass", StringType(), True),      # Clase de meteorito
    StructField("mass (g)", DoubleType(), True),      # Masa en gramos
    StructField("fall", StringType(), True),          # 'Fell' (observado) o 'Found' (encontrado)
    StructField("year", IntegerType(), True),         # Año de caída 
    StructField("reclat", StringType(), True),        # Latitud 
    StructField("reclong", StringType(), True),       # Longitud 
    StructField("GeoLocation", StringType(), True)    # Coordenadas en formato 'lat,long'
])

# Leer datos desde HDFS
df = spark.read.schema(schema) \
    .csv("hdfs://namenode:9000/user/luser/datasets/meteorites.csv", header=True)

# =========================================================

print("\n" + 60 * "=" + "\n")
print("Registros más antiguos:")

df_10_oldest = df.filter(col("year").isNotNull()) \
    .drop("GeoLocation") \
    .orderBy(asc("year")) \
    .limit(10)
df_10_oldest.show(truncate=False)

# =========================================================

print("\n" + 60 * "=" + "\n")
print("Eliminar geolocalización (redundante, ya que tenemos lat y long):")

df = df.drop("GeoLocation")

# Limpieza previa: eliminar espacios y manejar valores vacíos en coordenadas
df = df.withColumn("reclat", trim(col("reclat"))) \
       .withColumn("reclong", trim(col("reclong")))

df = df.withColumn("reclat", 
                   when((col("reclat") != "") & (col("reclat").isNotNull()), col("reclat"))
                   .otherwise(None)) \
       .withColumn("reclong", 
                   when((col("reclong") != "") & (col("reclong").isNotNull()), col("reclong"))
                   .otherwise(None))

# Convertir y renombrar columnas
df = df.withColumn("lat", col("reclat").cast(DoubleType())) \
       .withColumn("long", col("reclong").cast(DoubleType())) \
       .drop("reclat", "reclong")
df.show(10, truncate=False)

# =========================================================

print("\n" + 60 * "=" + "\n")
print("Añadir país a partir de coordenadas:")

# Mapeo de códigos de país de dos letras a nombres
def get_country_code_mapping():
    country_mapping = {}
    for country in pycountry.countries:
        country_mapping[country.alpha_2] = country.name
    return country_mapping

country_mapping = get_country_code_mapping()

# Definir UDF para obtener el nombre del país
def get_country(lat, lon):
    try:
        result = rg.search((lat, lon), mode='slow')  # Modo single-thread
        return country_mapping.get(result[0]['cc'], "Unknown")
    except Exception:
        return "Unknown"
    
# Solo hay dos países con coordenadas nulas, 

country_udf = udf(get_country, StringType())

# Aplicar al DataFrame
df = df.withColumn("country", country_udf(col("lat"), col("long")))
df.show(10, truncate=False)

# =========================================================

print("\n" + 60 * "=" + "\n")
print("Meteoritos más destructivos:")

# Criterios de destructividad supuestos:
# -Mayor masa (mass (g)): Más energía de impacto
# -Tipos metálicos (contienen 'Iron'): Mayor capacidad para penetrar la atmósfera sin desintegrarse

# Filtrar
destructive_meteorites = df.filter(
    (col("mass (g)").isNotNull()) &
    (col("recclass").contains("Iron"))
# Calcular score    
).withColumn("destruction_score", 
    coalesce(col("mass (g)"), lit(0)) * lit(1.75) * 
    when(col("recclass").contains("IIIAB"), 1.5)  
     .when(col("recclass").contains("IIAB"), 1.25)
     .otherwise(1)
).orderBy(desc("destruction_score"))

destructive_meteorites.show(10, truncate=False)

# =========================================================

print("\n" + 60 * "=" + "\n")
print("Países con más meteoritos destructivos registrados:")

destructive_meteorites.groupBy("country") \
    .agg(
        count("*").alias("total_impacts"),
        round(avg("mass (g)"), 2).alias("avg_mass"),
        round(max("mass (g)"), 2).alias("max_mass")
    ).orderBy(desc("total_impacts")) \
    .show(10, truncate=False)

# =========================================================

spark.stop()