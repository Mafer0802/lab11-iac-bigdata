from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Validar Gold - Categorias")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://ucv-data-platform/products")
    .config("spark.hadoop.fs.s3a.endpoint", "http://floci:4566")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

print("\n=== VALIDACION GOLD: UNA FILA POR CATEGORY ===")

df = spark.table("lakehouse.gold.product_category_summary")

total = df.count()

duplicadas = (
    df.groupBy("category")
    .count()
    .filter(F.col("count") > 1)
)

cantidad_duplicadas = duplicadas.count()

print("Total de filas Gold:", total)
print("Categorias distintas:", df.select("category").distinct().count())
print("Categorias duplicadas:", cantidad_duplicadas)

print("\nDetalle de la tabla Gold:")
df.show(50, truncate=False)

assert total > 0, "ERROR: Gold no contiene registros"
assert cantidad_duplicadas == 0, "ERROR: Existen categorias duplicadas"

print("\nVALIDACION GOLD: UNA FILA POR CATEGORY CORRECTA")

spark.stop()
