-- Olist Sales Dashboard Database Schema

-- Enable foreign key constraints
PRAGMA foreign_keys = ON; 
-- manatins data integrity 

-- Drop tables- for clean rebuilds when rerun 
DROP TABLE IF EXISTS fact_orders;
DROP TABLE IF EXISTS dim_order_items;
DROP TABLE IF EXISTS dim_payments;
DROP TABLE IF EXISTS dim_reviews;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_sellers;
DROP TABLE IF EXISTS dim_customers;
DROP TABLE IF EXISTS dim_geolocation;
DROP TABLE IF EXISTS dim_product_categories;
DROP TABLE IF EXISTS dim_economic_indicators;
DROP TABLE IF EXISTS dim_holidays;
DROP TABLE IF EXISTS dim_states;

-- Drop views for clean rebuilds
DROP VIEW IF EXISTS vw_order_analysis;
DROP VIEW IF EXISTS vw_regional_performance;

-- =============================================================================
-- DIMENSION TABLES 
-- =============================================================================

-- State Enhancement Data
CREATE TABLE dim_states (
    state_code TEXT PRIMARY KEY,
    state_name TEXT NOT NULL,
    region TEXT NOT NULL,
    population_2017 INTEGER,
    gdp_per_capita_2017 REAL,
    internet_penetration_pct REAL,
    higher_education_pct REAL,
    urbanization_rate REAL,
    area_km2 REAL,
    population_density REAL,
    economic_tier TEXT CHECK (economic_tier IN ('High', 'Upper-Middle', 'Middle', 'Lower-Middle')),
    is_major_metro INTEGER CHECK (is_major_metro IN (0, 1)),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Holiday and Temporal Data
CREATE TABLE dim_holidays (
    purchase_date DATE PRIMARY KEY,
    weekday INTEGER CHECK (weekday >= 0 AND weekday <= 6),
    month INTEGER CHECK (month >= 1 AND month <= 12),
    is_holiday INTEGER CHECK (is_holiday IN (0, 1)),
    holiday_name TEXT,
    is_carnival INTEGER CHECK (is_carnival IN (0, 1)),
    is_weekend INTEGER CHECK (is_weekend IN (0, 1)),
    is_friday INTEGER CHECK (is_friday IN (0, 1)),
    season TEXT CHECK (season IN ('Summer', 'Autumn', 'Winter', 'Spring')),
    christmas_season INTEGER CHECK (christmas_season IN (0, 1)),
    is_major_event INTEGER CHECK (is_major_event IN (0, 1)),
    is_shopping_holiday INTEGER CHECK (is_shopping_holiday IN (0, 1)),
    day_of_month INTEGER CHECK (day_of_month >= 1 AND day_of_month <= 31),
    is_mid_month INTEGER CHECK (is_mid_month IN (0, 1)),
    is_last_3_days INTEGER CHECK (is_last_3_days IN (0, 1)),
    is_day_24_non_december INTEGER CHECK (is_day_24_non_december IN (0, 1)),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Economic Indicators
CREATE TABLE dim_economic_indicators (
    date DATE PRIMARY KEY,
    usd_brl_rate REAL,
    selic_rate REAL,
    selic_target REAL,
    ipca_inflation REAL,
    usd_brl_change REAL,
    usd_brl_volatility REAL,
    usd_brl_30day_avg REAL,
    usd_brl_high INTEGER CHECK (usd_brl_high IN (0, 1)),
    usd_brl_low INTEGER CHECK (usd_brl_low IN (0, 1)),
    selic_changed INTEGER CHECK (selic_changed IN (0, 1)),
    high_interest_period INTEGER CHECK (high_interest_period IN (0, 1)),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Product Categories Translation
CREATE TABLE dim_product_categories (
    product_category_name TEXT PRIMARY KEY,
    product_category_name_english TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Geolocation Data
CREATE TABLE dim_geolocation (
    geolocation_zip_code_prefix INTEGER,
    geolocation_lat REAL,
    geolocation_lng REAL,
    geolocation_city TEXT,
    geolocation_state TEXT,
    PRIMARY KEY (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state),
    FOREIGN KEY (geolocation_state) REFERENCES dim_states(state_code)
);

-- Customers Dimension
CREATE TABLE dim_customers (
    customer_id TEXT PRIMARY KEY,
    customer_unique_id TEXT,
    customer_zip_code_prefix INTEGER,
    customer_city TEXT,
    customer_state TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_state) REFERENCES dim_states(state_code)
);

-- Sellers Dimension
CREATE TABLE dim_sellers (
    seller_id TEXT PRIMARY KEY,
    seller_zip_code_prefix INTEGER,
    seller_city TEXT,
    seller_state TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (seller_state) REFERENCES dim_states(state_code)
);

-- Products Dimension
CREATE TABLE dim_products (
    product_id TEXT PRIMARY KEY,
    product_category_name TEXT,
    product_name_lenght REAL,
    product_description_lenght REAL,
    product_photos_qty REAL,
    product_weight_g REAL,
    product_length_cm REAL,
    product_height_cm REAL,
    product_width_cm REAL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_category_name) REFERENCES dim_product_categories(product_category_name)
);

-- Reviews Dimension
CREATE TABLE dim_reviews (
    review_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    review_score INTEGER CHECK (review_score >= 1 AND review_score <= 5),
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Payments Dimension
CREATE TABLE dim_payments (
    order_id TEXT,
    payment_sequential INTEGER,
    payment_type TEXT,
    payment_installments INTEGER,
    payment_value REAL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, payment_sequential)
);

-- Order Items Dimension
CREATE TABLE dim_order_items (
    order_id TEXT,
    order_item_id INTEGER,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date DATETIME,
    price REAL,
    freight_value REAL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (seller_id) REFERENCES dim_sellers(seller_id)
);

-- =============================================================================
-- FACT TABLE (Main Transactional Data)
-- =============================================================================

-- Orders Fact Table
CREATE TABLE fact_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    order_status TEXT NOT NULL CHECK 
    (order_status IN ('delivered', 'shipped', 'processing', 'unavailable', 'canceled', 'approved', 'invoiced', 'created')),
    order_purchase_timestamp DATETIME NOT NULL CHECK (
        order_purchase_timestamp BETWEEN '2016-01-01' AND '2018-12-31'
    ),
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    
    -- Derived delivery metrics
    delivery_days INTEGER CHECK (delivery_days >= 0 AND delivery_days <= 365),
    delivery_vs_estimate INTEGER CHECK (delivery_vs_estimate >= -365 AND delivery_vs_estimate <= 365),
    on_time_delivery INTEGER CHECK (on_time_delivery IN (0, 1)),
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
   
    -- Foreign key relationships
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
);

-- =============================================================================
-- INDEXES FOR PERFORMANCE
-- =============================================================================

-- Date-based indexes for time series analysis 

--  Temporal sales analysis - tracks sales trends over time
CREATE INDEX idx_fact_orders_purchase_date ON fact_orders(DATE(order_purchase_timestamp));
--Holiday impact analysis - joins orders with holiday data to measure seasonal effects
CREATE INDEX idx_holidays_purchase_date ON dim_holidays(purchase_date);
-- joins orders with economic data to analyse how purchasing behaviours are affected
CREATE INDEX idx_economic_date ON dim_economic_indicators(date);

-- Geographic indexes
-- Regional performance analysis - geographic distribution and state-level comparisons
CREATE INDEX idx_customers_state ON dim_customers(customer_state);
CREATE INDEX idx_sellers_state ON dim_sellers(seller_state);
CREATE INDEX idx_customers_zip ON dim_customers(customer_zip_code_prefix);
CREATE INDEX idx_sellers_zip ON dim_sellers(seller_zip_code_prefix);

-- Product and category indexes
CREATE INDEX idx_products_category ON dim_products(product_category_name);
CREATE INDEX idx_order_items_product ON dim_order_items(product_id);
CREATE INDEX idx_order_items_seller ON dim_order_items(seller_id);

-- Order relationship indexes
CREATE INDEX idx_payments_order ON dim_payments(order_id);
CREATE INDEX idx_reviews_order ON dim_reviews(order_id); 
-- quality metrics?
CREATE INDEX idx_order_items_order ON dim_order_items(order_id); 
--possible basket analysis and order complexity analysis 

-- =============================================================================
-- VIEWS FOR COMMON ANALYTICAL QUERIES
-- =============================================================================

-- Comprehensive order analysis view
CREATE VIEW vw_order_analysis AS
SELECT 
    fo.order_id,
    fo.customer_id,
    fo.order_status,
    fo.order_purchase_timestamp,
    DATE(fo.order_purchase_timestamp) as order_date,
    
    -- Customer information
    dc.customer_state,
    dc.customer_city,
    
    -- Holiday and temporal context
    dh.weekday,
    dh.month,
    dh.season,
    dh.is_holiday,
    dh.is_weekend,
    dh.is_shopping_holiday,
    dh.christmas_season,
    
    -- Economic context
    de.usd_brl_rate,
    de.selic_target,
    de.usd_brl_volatility,
    
    -- State-level context
    ds.region,
    ds.gdp_per_capita_2017,
    ds.economic_tier,
    ds.internet_penetration_pct,
    
    -- Delivery performance
    fo.delivery_days,
    fo.on_time_delivery
    
FROM fact_orders fo
LEFT JOIN dim_customers dc ON fo.customer_id = dc.customer_id
LEFT JOIN dim_holidays dh ON DATE(fo.order_purchase_timestamp) = dh.purchase_date
LEFT JOIN dim_economic_indicators de ON DATE(fo.order_purchase_timestamp) = de.date
LEFT JOIN dim_states ds ON dc.customer_state = ds.state_code;

-- Regional sales performance view
CREATE VIEW vw_regional_performance AS
SELECT 
    ds.region,
    ds.state_code,
    ds.state_name,
    COUNT(fo.order_id) as total_orders,
    COUNT(DISTINCT fo.customer_id) as unique_customers,
    ds.population_2017,
    ds.gdp_per_capita_2017,
    ds.economic_tier
FROM dim_states ds
LEFT JOIN dim_customers dc ON ds.state_code = dc.customer_state
LEFT JOIN fact_orders fo ON dc.customer_id = fo.customer_id
GROUP BY ds.region, ds.state_code, ds.state_name, ds.population_2017, ds.gdp_per_capita_2017, ds.economic_tier;


-- =============================================================================
-- COMMENTS AND DOCUMENTATION
-- =============================================================================

SELECT 'Olist Sales Dashboard schema created successfully' AS status;