from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Validar Silver - Duplicados")
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

print("\n=== VALIDACION SILVER: PRODUCT_ID SIN DUPLICADOS ===")

df = spark.table("lakehouse.silver.products")

total = df.count()

duplicados = (
    df.groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
)

cantidad_duplicados = duplicados.count()

print("Total de registros:", total)
print("Cantidad de product_id duplicados:", cantidad_duplicados)

if cantidad_duplicados > 0:
    print("Detalle de duplicados:")
    duplicados.show(20, truncate=False)

assert cantidad_duplicados == 0, "ERROR: Existen product_id duplicados"

print("\nVALIDACION SILVER SIN DUPLICADOS CORRECTA")

spark.stop()
