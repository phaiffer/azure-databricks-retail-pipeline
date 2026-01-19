import os
from importlib import metadata
from pyspark.sql import SparkSession

# Configuração de caminhos para consistência com o pipeline
abs_base_path = os.path.abspath("data")
metastore_path = os.path.join(abs_base_path, "metastore_db")

# Inicialização formal da SparkSession para acessar o Metastore correto
spark = SparkSession.builder \
    .appName("DebugUtility") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", f"file://{abs_base_path}/warehouse") \
    .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={metastore_path};create=true") \
    .enableHiveSupport() \
    .getOrCreate()

def debug_storage():
    """
    Debug utility to check where data is actually stored.
    """
    print("=" * 60)
    print("DEBUG: STORAGE VERIFICATION")
    print("=" * 60)

    print("\nPHYSICAL FILES:")
    base_dirs = ["data/raw", "data/warehouse", "data/spark-warehouse"]

    for base_dir in base_dirs:
        if os.path.exists(base_dir):
            print(f"\n{base_dir}/")
            for root, dirs, files in os.walk(base_dir):
                level = root.replace(base_dir, '').count(os.sep)
                indent = ' ' * 2 * level
                print(f'{indent}{os.path.basename(root)}/')

                subindent = ' ' * 2 * (level + 1)
                important_files = [f for f in files if f.endswith(('.parquet', '.csv', '.json'))]
                for file in important_files[:10]:
                    size = os.path.getsize(os.path.join(root, file))
                    print(f'{subindent}{file} ({size:,} bytes)')

                if len(important_files) > 10:
                    print(f'{subindent}... and {len(important_files) - 10} more files')
        else:
            print(f"\nDirectory not found: {base_dir}")

    print("\n" + "=" * 60)
    print("SPARK METASTORE STATUS:")
    print("=" * 60)

    try:
        print("\nDatabases:")
        spark.sql("SHOW DATABASES").show(truncate=False)

        databases = ["bronze_retail", "silver_retail", "gold_retail"]
        for db in databases:
            print(f"\nTables in {db}:")
            try:
                spark.sql(f"USE {db}")
                tables_df = spark.sql("SHOW TABLES")
                if tables_df.count() > 0:
                    tables_df.show(truncate=False)
                    for row in tables_df.collect():
                        table_name = row.tableName
                        count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
                        print(f"  {table_name}: {count:,} records")
                else:
                    print(f"  No tables found in {db}")
            except Exception as e:
                print(f"  Error accessing {db}: {e}")
            finally:
                spark.sql("USE default")
    except Exception as e:
        print(f"Error checking Spark status: {e}")

def check_requirements():
    """Check if required packages are installed."""
    print("\n" + "=" * 60)
    print("REQUIREMENTS CHECK:")
    print("=" * 60)

    required = ['pyspark', 'requests', 'tenacity']
    for package in required:
        try:
            version = metadata.version(package)
            print(f"OK - {package}: {version}")
        except metadata.PackageNotFoundError:
            print(f"FAILED - {package}: NOT INSTALLED")

if __name__ == "__main__":
    check_requirements()
    debug_storage()
    if 'spark' in locals():
        spark.stop()