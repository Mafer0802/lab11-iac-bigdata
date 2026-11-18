from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Validar Silver - Nulos y Precios")
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

print("\n=== VALIDACION SILVER: product_id y price ===")

df = spark.table("lakehouse.silver.products")

total = df.count()

nulos_product_id = df.filter(
    F.col("product_id").isNull()
).count()

precios_invalidos = df.filter(
    F.col("price").isNull() | (F.col("price") <= 0)
).count()

print("Total de registros Silver:", total)
print("Registros con product_id nulo:", nulos_product_id)
print("Registros con price nulo o menor/igual a cero:", precios_invalidos)

assert total > 0, "ERROR: La tabla Silver no contiene registros"
assert nulos_product_id == 0, "ERROR: Existen product_id nulos"
assert precios_invalidos == 0, "ERROR: Existen precios nulos o menores/iguales a cero"

print("\nVALIDACION SILVER CORRECTA")
spark.stop()
