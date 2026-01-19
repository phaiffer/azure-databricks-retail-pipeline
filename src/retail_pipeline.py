from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, sum as _sum, desc, row_number, abs as _abs
from pyspark.sql.window import Window
import os


class RetailDataPipeline:
    """
    Modular Spark Pipeline with Medallion Architecture.
    Robust version with guaranteed data persistence.
    """

    def __init__(self, base_path="data"):
        # Network fix for Ubuntu - ensuring these are set before session creation
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
        os.environ["JAVA_OPTS"] = "-Djava.net.preferIPv4Stack=true"

        # Ensure base directory exists
        self.base_path = base_path
        self.abs_base_path = os.path.abspath(base_path)

        # Create ALL necessary directories
        self._create_directories()

        # MySQL Configuration (Update with your credentials)
        self.mysql_url = "jdbc:mysql://localhost:3306/retail_db"
        self.mysql_properties = {
            "user": "your_username",
            "password": "your_password",
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        # Configure Spark with EXPLICIT paths
        self.spark = self._create_spark_session()

        # Initialize schemas
        self._initialize_schemas()

    def _create_directories(self):
        """Create all required directories."""
        dirs = [
            f"{self.abs_base_path}/raw",
            f"{self.abs_base_path}/warehouse/bronze",
            f"{self.abs_base_path}/warehouse/silver",
            f"{self.abs_base_path}/warehouse/gold",
            f"{self.abs_base_path}/spark-warehouse"
        ]

        for dir_path in dirs:
            os.makedirs(dir_path, exist_ok=True)
            print(f"✓ Directory: {dir_path}")

    def _create_spark_session(self):
        """Create Spark session with proper Hive and JDBC configuration."""
        metastore_path = os.path.join(self.abs_base_path, "metastore_db")
    
        return SparkSession.builder \
            .appName("RetailPipeline") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", f"file://{self.abs_base_path}/warehouse") \
            .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={metastore_path};create=true") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.3.0") \
            .enableHiveSupport() \
            .getOrCreate()

    def _save_to_mysql(self, df, table_name):
        """Export a DataFrame to MySQL database."""
        print(f"-> Exporting {table_name} to MySQL...")
        try:
            df.write.jdbc(
                url=self.mysql_url,
                table=table_name,
                mode="overwrite",
                properties=self.mysql_properties
            )
            print(f"✓ Table {table_name} successfully updated in MySQL.")
        except Exception as e:
            print(f"⚠️ Error exporting to MySQL: {e}")
            print("Tip: Ensure the MySQL JDBC driver is available and credentials are correct.")

    def _initialize_schemas(self):
        """Create databases if they don't exist."""
        try:
            # Create databases
            self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze_retail")
            self.spark.sql("CREATE DATABASE IF NOT EXISTS silver_retail")
            self.spark.sql("CREATE DATABASE IF NOT EXISTS gold_retail")

            print("\n✓ Databases created:")
            self.spark.sql("SHOW DATABASES").show(truncate=False)

        except Exception as e:
            print(f"Warning creating schemas: {e}")

    def process_bronze(self):
        """Load raw data into Bronze layer."""
        print("\n--- 1. Processing Bronze Layer ---")

        input_path = f"{self.base_path}/raw/sales_transactions.csv"
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Data not found: {input_path}")

        # Read raw data
        raw_df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(input_path)

        # Add ingestion timestamp
        bronze_df = raw_df.withColumn("ingestion_timestamp", current_timestamp())

        # Save to bronze_retail database
        bronze_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .saveAsTable("bronze_retail.sales_transactions")

        # Also save as parquet files for backup
        bronze_df.write \
            .mode("overwrite") \
            .parquet(f"{self.abs_base_path}/warehouse/bronze/sales_transactions")

        count = bronze_df.count()
        print(f"✓ Bronze layer: {count:,} records saved")

        return bronze_df

    def process_silver(self):
        """Clean and transform data in Silver layer."""
        print("\n--- 2. Processing Silver Layer ---")

        try:
            # Read from bronze table
            bronze_df = self.spark.table("bronze_retail.sales_transactions")
        except Exception as e:
            print(f"Error reading bronze table: {e}")
            return None

        # Data cleaning and transformation
        cleaned_df = bronze_df.select(
            col("transaction_id"),
            to_date(col("date"), "yyyy-MM-dd HH:mm:ss").alias("transaction_date"),
            col("customer_email"),
            col("category"),
            col("product_id").cast("integer"),
            col("price").cast("double"),
            col("quantity").cast("integer"),
            col("total_amount").cast("double"),
            col("payment_type"),
            col("ingestion_timestamp")
        )

        # Fix negative values
        cleaned_df = cleaned_df.withColumn("total_amount", _abs(col("total_amount")))

        # Remove duplicates
        window_spec = Window.partitionBy("transaction_id").orderBy("transaction_date")
        dedup_df = cleaned_df.withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")

        # Save to silver_retail database
        dedup_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .saveAsTable("silver_retail.sales_clean")

        # Also save as parquet files
        dedup_df.write \
            .mode("overwrite") \
            .parquet(f"{self.abs_base_path}/warehouse/silver/sales_clean")

        count = dedup_df.count()
        print(f"✓ Silver layer: {count:,} cleaned records")
        return dedup_df

    def process_gold(self):
        """Create aggregated KPIs in Gold layer."""
        print("\n--- 3. Processing Gold Layer ---")

        try:
            silver_df = self.spark.table("silver_retail.sales_clean")
        except Exception as e:
            print(f"Error reading silver table: {e}")
            return

        # Load users for enrichment
        users_path = f"{self.base_path}/raw/users.json"
        if os.path.exists(users_path):
            users_df = self.spark.read.option("multiline", "true").json(users_path)
            users_dim = users_df.select(
                col("email").alias("customer_email"),
                col("location.country").alias("country")
            )

            # Join with sales data
            enriched_df = silver_df.join(users_dim, "customer_email", "left")
        else:
            enriched_df = silver_df
            enriched_df = enriched_df.withColumn("country", col("unknown"))

        # Create KPIs: revenue by country and category
        kpi_df = enriched_df.groupBy("country", "category") \
            .agg(_sum("total_amount").alias("revenue")) \
            .orderBy(desc("revenue"))

        # Save to gold_retail database
        kpi_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .saveAsTable("gold_retail.category_performance")

        # Also save as parquet files
        kpi_df.write \
            .mode("overwrite") \
            .parquet(f"{self.abs_base_path}/warehouse/gold/category_performance")

        # Export to MySQL for Dashboarding
        self._save_to_mysql(kpi_df, "category_performance")

        print(f" Gold layer: {kpi_df.count():,} aggregated records")

    def verify_data(self):
        """Verify data is accessible in all layers."""
        print("\n" + "=" * 60)
        print("DATA VERIFICATION")
        print("=" * 60)

        tables = [
            ("bronze_retail", "sales_transactions"),
            ("silver_retail", "sales_clean"),
            ("gold_retail", "category_performance")
        ]

        for db, table in tables:
            try:
                query = f"SELECT COUNT(*) as count FROM {db}.{table}"
                result = self.spark.sql(query).collect()[0]["count"]
                print(f"✓ {db}.{table}: {result:,} records")
            except Exception as e:
                print(f"✗ {db}.{table}: ERROR - {e}")

    def run(self):
        """Execute the complete pipeline."""
        self.process_bronze()
        self.process_silver()
        self.process_gold()
        self.verify_data()
        print("\n Pipeline Completed Successfully!")

    def export_to_mysql(self, df, database_name, table_name):
        """
        Helper method to export DataFrames to specific Medallion databases in MySQL.
        """
        # Configurações de conexão (Idealmente viriam de variáveis de ambiente)
        jdbc_url = f"jdbc:mysql://localhost:3306/{database_name}"
        
        connection_properties = {
            "user": "root",      # <--- COLOQUE SEU USUÁRIO AQUI
            "password": "W!ll!@n55361316",    # <--- COLOQUE SUA SENHA AQUI
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        try:
            print(f"Sending data to {database_name}.{table_name}...")
            df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode="overwrite",
                properties=connection_properties
            )
            print(f" Success: {table_name} exported to {database_name}")
        except Exception as e:
            print(f" Failed to export to {database_name}: {e}")

    def run(self):
        """Execute the complete pipeline."""
        self.process_bronze()
        self.process_silver()
        self.process_gold()
        self.verify_data()
        print("\n Pipeline Completed Successfully!")
