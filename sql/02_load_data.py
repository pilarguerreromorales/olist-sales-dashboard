# python script more flexible for loading, handles messy data 

import pandas as pd
import sqlite3
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OlistDataLoader:
    def __init__(self, db_path='olist_dashboard.db', data_folder='data'):
        self.db_path = db_path
        self.data_folder = data_folder
        self.conn = None
        
    def connect(self):
        """Connect to SQLite database"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")
        logger.info(f"Connected to database: {self.db_path}")
        
    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def load_states(self):
        """Load dim_states from state_enhancement_documented.csv"""
        logger.info("Loading dim_states...")
        
        df = pd.read_csv(f'{self.data_folder}/state_enhancement_documented.csv')
        
        # Insert data
        df.to_sql('dim_states', self.conn, if_exists='append', index=False)
        
        count = len(df)
        logger.info(f"Loaded {count} states")
        return count
    
    def load_product_categories(self):
        """Load dim_product_categories from product_category_name_translation.csv + missing categories"""
        logger.info("Loading dim_product_categories...")
        
        df = pd.read_csv(f'{self.data_folder}/product_category_name_translation.csv')
        
        # Add missing categories that exist in products but not in translation
        missing_categories = pd.DataFrame([
            {'product_category_name': 'pc_gamer', 'product_category_name_english': 'pc_gaming'},
            {'product_category_name': 'portateis_cozinha_e_preparadores_de_alimentos', 'product_category_name_english': 'portable_kitchen_appliances'},
            {'product_category_name': 'unknown', 'product_category_name_english': 'unknown'}
        ])
        
        # Combine original categories with missing ones
        df = pd.concat([df, missing_categories], ignore_index=True)
        
        # Insert data
        df.to_sql('dim_product_categories', self.conn, if_exists='append', index=False)
        
        count = len(df)
        logger.info(f"Loaded {count} product categories (including 3 missing categories)")
        return count
    
    def load_products(self):
        """Load dim_products from olist_products_dataset.csv - FIXED VERSION"""
        logger.info("Loading dim_products...")
        
        df = pd.read_csv(f'{self.data_folder}/olist_products_dataset.csv')
        
        # Replace NULL categories with 'unknown'
        null_count = df['product_category_name'].isnull().sum()
        df['product_category_name'] = df['product_category_name'].fillna('unknown')
        
        initial_count = len(df)
        logger.info(f"Loading {initial_count} products ({null_count} NULL categories converted to 'unknown')")
        
        # Insert data
        df.to_sql('dim_products', self.conn, if_exists='append', index=False)
        
        count = len(df)
        logger.info(f"Loaded {count} products")
        return count
    
    def load_customers(self):
        """Load dim_customers from olist_customers_dataset.csv"""
        logger.info("Loading dim_customers...")
        
        df = pd.read_csv(f'{self.data_folder}/olist_customers_dataset.csv')
        
        # Insert data (note: will fail if state codes don't exist in dim_states)
        df.to_sql('dim_customers', self.conn, if_exists='append', index=False)
        
        count = len(df)
        logger.info(f"Loaded {count} customers")
        return count
    
    def load_sellers(self):
        """Load dim_sellers from olist_sellers_dataset.csv"""
        logger.info("Loading dim_sellers...")
        
        df = pd.read_csv(f'{self.data_folder}/olist_sellers_dataset.csv')
        
        # Insert data
        df.to_sql('dim_sellers', self.conn, if_exists='append', index=False)
        
        count = len(df)
        logger.info(f"Loaded {count} sellers")
        return count
    
    
    def calculate_delivery_metrics(self, df):
        """Calculate derived delivery metrics"""
        logger.info("Calculating delivery metrics...")
        
        # Convert date columns to datetime
        date_cols = ['order_purchase_timestamp', 'order_approved_at', 
                    'order_delivered_carrier_date', 'order_delivered_customer_date', 
                    'order_estimated_delivery_date']
        
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Calculate delivery_days (purchase to customer delivery)
        df['delivery_days'] = (
            df['order_delivered_customer_date'] - df['order_purchase_timestamp']
        ).dt.days
        
        # Calculate delivery_vs_estimate (actual vs estimated)
        df['delivery_vs_estimate'] = (
            df['order_delivered_customer_date'] - df['order_estimated_delivery_date']
        ).dt.days
        
        # Calculate on_time_delivery (1 if on time or early, 0 if late)
        df['on_time_delivery'] = (df['delivery_vs_estimate'] <= 0).astype(int)
        
        # Handle cases where delivery hasn't happened yet
        df['delivery_days'] = df['delivery_days'].where(df['delivery_days'] >= 0)
        df['on_time_delivery'] = df['on_time_delivery'].where(df['order_delivered_customer_date'].notna())
        
        logger.info(f"Calculated metrics for {len(df)} orders")
        logger.info(f"Average delivery time: {df['delivery_days'].mean():.1f} days")
        logger.info(f"On-time delivery rate: {df['on_time_delivery'].mean()*100:.1f}%")
        
        return df
    
    def load_orders(self):
        """Load fact_orders from olist_orders_dataset2.csv with derived metrics"""
        logger.info("Loading fact_orders...")
        
        df = pd.read_csv(f'{self.data_folder}/olist_orders_dataset2.csv')
        
        # Calculate derived metrics
        df = self.calculate_delivery_metrics(df)
        
        # Select only columns that match our schema (exclude purchase_date as it's redundant)
        columns_to_load = [
            'order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
            'order_approved_at', 'order_delivered_carrier_date', 
            'order_delivered_customer_date', 'order_estimated_delivery_date',
            'delivery_days', 'delivery_vs_estimate', 'on_time_delivery'
        ]
        
        df_to_load = df[columns_to_load].copy()
        
        # Insert data
        df_to_load.to_sql('fact_orders', self.conn, if_exists='append', index=False)
        
        count = len(df_to_load)
        logger.info(f"Loaded {count} orders")
        return count
    
    def load_order_items(self):
        """Load dim_order_items from olist_order_items_dataset.csv"""
        logger.info("Loading dim_order_items...")
        
        df = pd.read_csv(f'{self.data_folder}/olist_order_items_dataset.csv')
        
        # Convert shipping_limit_date to datetime
        df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'])
        
        # Insert data
        df.to_sql('dim_order_items', self.conn, if_exists='append', index=False)
        
        count = len(df)
        logger.info(f"Loaded {count} order items")
        return count
    
    def load_payments(self):
        """Load dim_payments from olist_order_payments_dataset.csv"""
        logger.info("Loading dim_payments...")
        
        df = pd.read_csv(f'{self.data_folder}/olist_order_payments_dataset.csv')
        
        # Insert data
        df.to_sql('dim_payments', self.conn, if_exists='append', index=False)
        
        count = len(df)
        logger.info(f"Loaded {count} payments")
        return count
    
    def load_reviews(self):
        """Load dim_reviews from olist_order_reviews_dataset.csv"""
        logger.info("Loading dim_reviews...")
        
        df = pd.read_csv(f'{self.data_folder}/olist_order_reviews_dataset.csv')
        
        # Convert date columns
        df['review_creation_date'] = pd.to_datetime(df['review_creation_date'])
        df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'])
        
        # Insert data
        df.to_sql('dim_reviews', self.conn, if_exists='append', index=False)
        
        count = len(df)
        logger.info(f"Loaded {count} reviews")
        return count
    
    def load_holidays(self):
        """Load dim_holidays from olist_holiday_dataset.csv"""
        logger.info("Loading dim_holidays...")
        
        df = pd.read_csv(f'{self.data_folder}/olitst_holiday_dataset.csv')
        
        # Convert purchase_date to date format
        df['purchase_date'] = pd.to_datetime(df['purchase_date']).dt.date
        
        schema_columns = [
            'purchase_date', 'weekday', 'month', 'is_holiday', 'holiday_name',
            'is_carnival', 'is_weekend', 'is_friday', 'season', 'christmas_season',
            'is_major_event', 'is_shopping_holiday', 'day_of_month', 'is_mid_month',
            'is_last_3_days', 'is_day_24_non_december'
        ]
        
        # Only include columns that exist in both the CSV and our schema
        available_columns = [col for col in schema_columns if col in df.columns]
        df_to_load = df[available_columns].copy()
        
        # Insert data
        df_to_load.to_sql('dim_holidays', self.conn, if_exists='append', index=False)
        
        count = len(df_to_load)
        logger.info(f"Loaded {count} holiday records")
        return count
    
    def load_economic_indicators(self):
        """Load dim_economic_indicators from economic_indicators.csv"""
        logger.info("Loading dim_economic_indicators...")
        
        df = pd.read_csv(f'{self.data_folder}/economic_indicators.csv')
        
        # Convert purchase_date to date and rename to date
        df['date'] = pd.to_datetime(df['purchase_date']).dt.date
        df = df.drop('purchase_date', axis=1)
        
        # Insert data
        df.to_sql('dim_economic_indicators', self.conn, if_exists='append', index=False)
        
        count = len(df)
        logger.info(f"Loaded {count} economic indicator records")
        return count
    
    def load_geolocation(self):
        """Load dim_geolocation from olist_geolocation_dataset.csv"""
        logger.info("Loading dim_geolocation...")
        
        df = pd.read_csv(f'{self.data_folder}/olist_geolocation_dataset.csv')
        
        # Insert data
        df.to_sql('dim_geolocation', self.conn, if_exists='append', index=False)
        
        count = len(df)
        logger.info(f"Loaded {count} geolocation records")
        return count
    
    def verify_data(self):
        """Verify data was loaded correctly"""
        logger.info("Verifying data load...")
        
        cursor = self.conn.cursor()
        
        # Check table counts
        tables = [
            'dim_states', 'dim_product_categories', 'dim_customers', 'dim_sellers',
            'dim_products', 'fact_orders', 'dim_order_items', 'dim_payments',
            'dim_reviews', 'dim_holidays', 'dim_economic_indicators', 'dim_geolocation'
        ]
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logger.info(f"{table}: {count:,} records")
        
        # Test a view
        cursor.execute("SELECT COUNT(*) FROM vw_order_analysis")
        view_count = cursor.fetchone()[0]
        logger.info(f"vw_order_analysis view: {view_count:,} records")
        
        # Test foreign key constraints
        cursor.execute("""
            SELECT COUNT(*) FROM fact_orders fo 
            LEFT JOIN dim_customers dc ON fo.customer_id = dc.customer_id 
            WHERE dc.customer_id IS NULL
        """)
        orphaned_orders = cursor.fetchone()[0]
        
        if orphaned_orders > 0:
            logger.warning(f"Found {orphaned_orders} orders with missing customers")
        else:
            logger.info("All orders have valid customer references")
    
    def load_all_data(self):
        """Load all data in the correct order"""
        try:
            self.connect()
            
            logger.info("Starting data load process...")
            start_time = datetime.now()
            
            # Load in dependency order
            self.load_states()
            self.load_product_categories()
            self.load_customers()
            self.load_sellers()
            self.load_products()
            self.load_orders()  # This includes derived metrics calculation
            self.load_order_items()
            self.load_payments()
            self.load_reviews()
            self.load_holidays()
            self.load_economic_indicators()
            self.load_geolocation()
            
            # Commit all changes
            self.conn.commit()
            
            # Verify the load
            self.verify_data()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"Data load completed successfully in {duration:.1f} seconds")
            
        except Exception as e:
            logger.error(f"Data load failed: {str(e)}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            self.disconnect()

if __name__ == "__main__":
    # Run the data loader
    loader = OlistDataLoader()
    loader.load_all_data()