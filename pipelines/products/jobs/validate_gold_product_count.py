from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Validar Gold - Product Count")
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

print("\n=== VALIDACION GOLD: PRODUCT_COUNT MAYOR A CERO ===")

df = spark.table("lakehouse.gold.product_category_summary")

total = df.count()

invalidos = df.filter(
    F.col("product_count").isNull() | (F.col("product_count") <= 0)
).count()

print("Total de registros Gold:", total)
print("Registros con product_count nulo o menor/igual a cero:", invalidos)

print("\nDetalle de productos por categoria:")
df.select("category", "product_count").show(50, truncate=False)

assert total > 0, "ERROR: La tabla Gold no contiene registros"
assert invalidos == 0, "ERROR: Existen valores de product_count no validos"

print("\nVALIDACION GOLD PRODUCT_COUNT CORRECTA")

spark.stop()
