import os
import shutil
from src.ingestion_service import DataIngestionService
from src.retail_pipeline import RetailDataPipeline


def clean_environment():
    """
    Clean local warehouse and metastore to ensure fresh execution.
    """
    # Removido "metastore_db" e "derby.log" da lista para manter o catálogo vivo
    paths_to_clean = ["data/spark-warehouse"]

    for p in paths_to_clean:
        if os.path.exists(p):
            try:
                if os.path.isdir(p):
                    shutil.rmtree(p)
                else:
                    os.remove(p)
                print(f"Cleanup completed: {p}")
            except Exception as e:
                print(f"Warning cleaning {p}: {e}")


if __name__ == "__main__":
    print("Starting Data Engineering Job...")

    # 1. Clean previous execution artifacts
    clean_environment()

    # 2. Data Ingestion (if raw data doesn't exist)
    if not os.path.exists("data/raw/sales_transactions.csv"):
        print("Raw data not found. Starting API Ingestion...")
        ingestor = DataIngestionService()
        try:
            products = ingestor.ingest_products_data()
            users = ingestor.ingest_users_data(count=150)
            ingestor.generate_sales_transactions(products, users, num_transactions=3000)
        except Exception as e:
            print(f"Critical ingestion failure: {e}")
            exit(1)
    else:
        print("Raw data found. Skipping ingestion.")

    # 3. Spark ETL Pipeline
    print("\nStarting Spark Pipeline (Lakehouse)...")
    pipeline = RetailDataPipeline(base_path="data")

    try:
        pipeline.run()

        # 4. Success verification
        print("\n" + "=" * 50)
        print("FINAL EXECUTION REPORT")
        print("=" * 50)

        # Show physical files
        print("\n📁 Physical files created:")
        import glob

        parquet_files = glob.glob("data/warehouse/**/*.parquet", recursive=True)
        for file in parquet_files:
            print(f"  ✓ {file}")

        if not parquet_files:
            print("  ⚠️ No Parquet files found!")

    finally:
        pipeline.spark.stop()