import requests
import json
import csv
import random
import logging
from pathlib import Path
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataIngestionService:
    """
    Service for extracting data from public APIs, applying error handling,
    and saving to Raw Layer (Landing Zone).
    """

    def __init__(self, base_output_dir="data/raw"):
        self.base_output_dir = Path(base_output_dir)
        self.base_output_dir.mkdir(parents=True, exist_ok=True)

        # Public API endpoints
        self.products_api = "https://fakestoreapi.com/products"
        self.users_api = "https://randomuser.me/api/"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _fetch_data(self, url, params=None):
        """
        HTTP request with Exponential Backoff (Resilience pattern).
        """
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API connection error {url}: {e}")
            raise

    def ingest_products_data(self):
        """Fetch products data from FakeStoreAPI."""
        logger.info("Fetching Products data...")
        products_data = self._fetch_data(self.products_api)

        output_file = self.base_output_dir / "products.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(products_data, f, indent=2)

        logger.info(f"{len(products_data)} products saved to {output_file}")
        return products_data

    def ingest_users_data(self, count=100):
        """Fetch users data from RandomUser API."""
        logger.info(f"Fetching {count} Users...")
        params = {'results': count, 'nat': 'br,us,gb,fr'}
        data = self._fetch_data(self.users_api, params=params)
        users_list = data.get('results', [])

        output_file = self.base_output_dir / "users.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(users_list, f, indent=2)

        logger.info(f"{len(users_list)} users saved to {output_file}")
        return users_list

    def generate_sales_transactions(self, products, users, num_transactions=2000):
        """
        Generate sales transactions by crossing real Products and Users.
        Simulates a POS system.
        """
        logger.info(f"Generating {num_transactions} sales transactions...")

        output_file = self.base_output_dir / "sales_transactions.csv"
        headers = ["transaction_id", "date", "customer_email", "product_id", "category", "price", "quantity",
                   "total_amount", "payment_type"]
        payment_types = ["Credit Card", "Debit Card", "Pix", "PayPal"]

        with open(output_file, "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)

            for i in range(num_transactions):
                product = random.choice(products)
                user = random.choice(users)

                txn_id = f"TXN-{100000 + i}"
                date = (datetime.now() - timedelta(days=random.randint(0, 60))).strftime("%Y-%m-%d %H:%M:%S")
                cust_email = user['email']

                # Business rule: Quantity and Total calculation
                qty = random.randint(1, 5)
                price = float(product['price'])
                total = round(price * qty, 2)

                # Intentional dirty data for ETL to clean
                if i % 50 == 0:
                    total = -total  # Negative value (system error)

                writer.writerow([txn_id, date, cust_email, product['id'], product['category'], price, qty, total,
                                 random.choice(payment_types)])

        logger.info(f"Transactions saved to {output_file}")