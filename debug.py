import os
import glob
from pyspark.sql import SparkSession


def debug_storage():
    """
    Debug utility to check where data is actually stored.
    Run this after main.py to verify data persistence.
    """
    print("=" * 60)
    print("DEBUG: STORAGE VERIFICATION")
    print("=" * 60)

    # Check physical files
    print("\n📁 PHYSICAL FILES:")

    base_dirs = ["data/raw", "data/warehouse", "data/spark-warehouse"]

    for base_dir in base_dirs:
        if os.path.exists(base_dir):
            print(f"\n{base_dir}/")
            for root, dirs, files in os.walk(base_dir):
                level = root.replace(base_dir, '').count(os.sep)
                indent = ' ' * 2 * level
                print(f'{indent}{os.path.basename(root)}/')

                # Show important files
                subindent = ' ' * 2 * (level + 1)
                important_files = [f for f in files if f.endswith(('.parquet', '.csv', '.json'))]
                for file in important_files[:10]:  # Show up to 10 files
                    size = os.path.getsize(os.path.join(root, file))
                    print(f'{subindent}{file} ({size:,} bytes)')

                if len(important_files) > 10:
                    print(f'{subindent}... and {len(important_files) - 10} more files')
        else:
            print(f"\n❌ Directory not found: {base_dir}")

    # Check Spark metastore
    print("\n" + "=" * 60)
    print("SPARK METASTORE STATUS:")
    print("=" * 60)

    spark = None
    try:
        spark = SparkSession.builder \
            .appName("Debug") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "file://" + os.path.abspath("data/warehouse")) \
            .enableHiveSupport() \
            .getOrCreate()

        print("\n📊 Databases:")
        spark.sql("SHOW DATABASES").show(truncate=False)

        databases = ["bronze_retail", "silver_retail", "gold_retail"]
        for db in databases:
            print(f"\n📈 Tables in {db}:")
            try:
                spark.sql(f"USE {db}")
                tables_df = spark.sql("SHOW TABLES")
                if tables_df.count() > 0:
                    tables_df.show(truncate=False)

                    # Show record counts
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
        print(f"Error creating Spark session: {e}")
    finally:
        if spark:
            spark.stop()


def check_requirements():
    """Check if required packages are installed."""
    print("\n" + "=" * 60)
    print("REQUIREMENTS CHECK:")
    print("=" * 60)

    required = ['pyspark', 'requests', 'tenacity']

    for package in required:
        try:
            if package == 'pyspark':
                import pyspark
                version = pyspark.__version__
            elif package == 'requests':
                import requests
                version = requests.__version__
            elif package == 'tenacity':
                import tenacity
                version = tenacity.__version__

            print(f"✓ {package}: {version}")
        except ImportError:
            print(f"✗ {package}: NOT INSTALLED")


if __name__ == "__main__":
    check_requirements()
    debug_storage()