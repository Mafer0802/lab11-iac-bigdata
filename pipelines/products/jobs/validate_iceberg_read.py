from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Validar lectura de tablas Iceberg")
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

print("\n=== VALIDACION DE TABLAS ICEBERG ===")

tablas = [
    "lakehouse.silver.products",
    "lakehouse.gold.product_category_summary"
]

for nombre in tablas:
    print("\nTabla:", nombre)

    df = spark.table(nombre)

    print("Cantidad de registros:", df.count())
    print("Columnas:", df.columns)

    df.show(5, truncate=False)

    print("LECTURA CORRECTA:", nombre)

print("\nVALIDACION ICEBERG CORRECTA: TABLAS RELEIDAS CON SPARK.TABLE()")

spark.stop()
