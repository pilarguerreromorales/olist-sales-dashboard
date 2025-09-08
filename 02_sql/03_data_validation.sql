-- Focused on data validation checks rather than cleaning. Lots of cleaning was handled during loading.
-- sqlite3 olist_dashboard.db < 02_sql/03_data_validation.sql
-- Olist Sales Dashboard Data Validation Script
-- File: 03_data_validation.sql
-- Purpose: Validate data quality and integrity after loading
-- Run after: 02_load_data.py

-- Enable foreign key constraints for validation
PRAGMA foreign_keys = ON;

-- =============================================================================
-- DATA COMPLETENESS VALIDATION
-- =============================================================================

-- Check record counts across all tables
SELECT 'DATA COMPLETENESS CHECK' as validation_type;

SELECT 
    'dim_states' as table_name, 
    COUNT(*) as record_count,
    CASE WHEN COUNT(*) = 27 THEN 'PASS' ELSE 'FAIL' END as status
FROM dim_states

UNION ALL

SELECT 
    'dim_product_categories', 
    COUNT(*),
    CASE WHEN COUNT(*) >= 71 THEN 'PASS' ELSE 'FAIL' END
FROM dim_product_categories

UNION ALL

SELECT 
    'dim_customers', 
    COUNT(*),
    CASE WHEN COUNT(*) > 90000 THEN 'PASS' ELSE 'FAIL' END
FROM dim_customers

UNION ALL

SELECT 
    'dim_sellers', 
    COUNT(*),
    CASE WHEN COUNT(*) > 3000 THEN 'PASS' ELSE 'FAIL' END
FROM dim_sellers

UNION ALL

SELECT 
    'dim_products', 
    COUNT(*),
    CASE WHEN COUNT(*) > 30000 THEN 'PASS' ELSE 'FAIL' END
FROM dim_products

UNION ALL

SELECT 
    'fact_orders', 
    COUNT(*),
    CASE WHEN COUNT(*) > 90000 THEN 'PASS' ELSE 'FAIL' END
FROM fact_orders

UNION ALL

SELECT 
    'dim_order_items', 
    COUNT(*),
    CASE WHEN COUNT(*) > 100000 THEN 'PASS' ELSE 'FAIL' END
FROM dim_order_items

UNION ALL

SELECT 
    'dim_payments', 
    COUNT(*),
    CASE WHEN COUNT(*) > 100000 THEN 'PASS' ELSE 'FAIL' END
FROM dim_payments

UNION ALL

SELECT 
    'dim_reviews', 
    COUNT(*),
    CASE WHEN COUNT(*) > 90000 THEN 'PASS' ELSE 'FAIL' END
FROM dim_reviews

UNION ALL

SELECT 
    'dim_holidays', 
    COUNT(*),
    CASE WHEN COUNT(*) > 700 THEN 'PASS' ELSE 'FAIL' END
FROM dim_holidays

UNION ALL

SELECT 
    'dim_economic_indicators', 
    COUNT(*),
    CASE WHEN COUNT(*) > 700 THEN 'PASS' ELSE 'FAIL' END
FROM dim_economic_indicators

UNION ALL

SELECT 
    'dim_geolocation', 
    COUNT(*),
    CASE WHEN COUNT(*) > 700000 THEN 'PASS' ELSE 'FAIL' END
FROM dim_geolocation;

-- =============================================================================
-- REFERENTIAL INTEGRITY VALIDATION
-- =============================================================================

SELECT '' as separator, 'REFERENTIAL INTEGRITY CHECK' as validation_type;

-- Check for orphaned records (foreign key violations)
SELECT 
    'Orders with missing customers' as check_description,
    COUNT(*) as violation_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM fact_orders fo 
LEFT JOIN dim_customers dc ON fo.customer_id = dc.customer_id 
WHERE dc.customer_id IS NULL

UNION ALL

SELECT 
    'Customers with invalid state codes',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dim_customers dc 
LEFT JOIN dim_states ds ON dc.customer_state = ds.state_code 
WHERE ds.state_code IS NULL

UNION ALL

SELECT 
    'Sellers with invalid state codes',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dim_sellers ds_seller 
LEFT JOIN dim_states ds ON ds_seller.seller_state = ds.state_code 
WHERE ds.state_code IS NULL

UNION ALL

SELECT 
    'Products with invalid categories',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dim_products dp 
LEFT JOIN dim_product_categories dpc ON dp.product_category_name = dpc.product_category_name 
WHERE dpc.product_category_name IS NULL

UNION ALL

SELECT 
    'Order items with missing products',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dim_order_items doi 
LEFT JOIN dim_products dp ON doi.product_id = dp.product_id 
WHERE dp.product_id IS NULL

UNION ALL

SELECT 
    'Order items with missing sellers',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dim_order_items doi 
LEFT JOIN dim_sellers ds ON doi.seller_id = ds.seller_id 
WHERE ds.seller_id IS NULL

UNION ALL

SELECT 
    'Order items with missing orders',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dim_order_items doi 
LEFT JOIN fact_orders fo ON doi.order_id = fo.order_id 
WHERE fo.order_id IS NULL

UNION ALL

SELECT 
    'Payments with missing orders',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dim_payments dp 
LEFT JOIN fact_orders fo ON dp.order_id = fo.order_id 
WHERE fo.order_id IS NULL

UNION ALL

SELECT 
    'Reviews with missing orders',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dim_reviews dr 
LEFT JOIN fact_orders fo ON dr.order_id = fo.order_id 
WHERE fo.order_id IS NULL;

-- =============================================================================
-- DATA QUALITY VALIDATION
-- =============================================================================

SELECT '' as separator, 'DATA QUALITY CHECK' as validation_type;

-- Check for NULL values in critical fields
SELECT 
    'Orders with NULL customer_id' as check_description,
    COUNT(*) as violation_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM fact_orders 
WHERE customer_id IS NULL

UNION ALL

SELECT 
    'Orders with NULL order_status',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM fact_orders 
WHERE order_status IS NULL

UNION ALL

SELECT 
    'Orders with NULL purchase_timestamp',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM fact_orders 
WHERE order_purchase_timestamp IS NULL

UNION ALL

SELECT 
    'Products with NULL categories (should be unknown)',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dim_products 
WHERE product_category_name IS NULL

UNION ALL

SELECT 
    'Customers with NULL state codes',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dim_customers 
WHERE customer_state IS NULL

UNION ALL

SELECT 
    'Reviews with invalid scores (not 1-5)',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM dim_reviews 
WHERE review_score NOT BETWEEN 1 AND 5;

-- =============================================================================
-- BUSINESS LOGIC VALIDATION
-- =============================================================================

SELECT '' as separator, 'BUSINESS LOGIC CHECK' as validation_type;

-- Check order status distribution
SELECT 
    'Order status distribution' as check_description,
    order_status,
    COUNT(*) as order_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_orders), 2) as percentage
FROM fact_orders 
GROUP BY order_status 
ORDER BY order_count DESC;

-- Check delivery performance metrics
SELECT 
    'Delivery performance summary' as check_description,
    COUNT(*) as total_orders,
    COUNT(delivery_days) as orders_with_delivery_days,
    ROUND(AVG(delivery_days), 1) as avg_delivery_days,
    ROUND(AVG(on_time_delivery) * 100, 1) as on_time_delivery_pct
FROM fact_orders;

-- Check for unrealistic delivery times
SELECT 
    'Orders with unrealistic delivery times' as check_description,
    COUNT(*) as violation_count,
    CASE WHEN COUNT(*) < 100 THEN 'PASS' ELSE 'REVIEW' END as status
FROM fact_orders 
WHERE delivery_days > 100 OR delivery_days < 0;

-- Check payment values
SELECT 
    'Payment validation' as check_description,
    COUNT(*) as total_payments,
    COUNT(CASE WHEN payment_value <= 0 THEN 1 END) as zero_or_negative_payments,
    ROUND(MIN(payment_value), 2) as min_payment,
    ROUND(MAX(payment_value), 2) as max_payment,
    ROUND(AVG(payment_value), 2) as avg_payment
FROM dim_payments;

-- Check for duplicate primary keys (should be 0)
SELECT 
    'Duplicate order IDs in fact_orders' as check_description,
    COUNT(*) - COUNT(DISTINCT order_id) as duplicate_count,
    CASE WHEN COUNT(*) = COUNT(DISTINCT order_id) THEN 'PASS' ELSE 'FAIL' END as status
FROM fact_orders

UNION ALL

SELECT 
    'Duplicate review IDs in dim_reviews',
    COUNT(*) - COUNT(DISTINCT review_id),
    CASE WHEN COUNT(*) = COUNT(DISTINCT review_id) THEN 'PASS' ELSE 'FAIL' END
FROM dim_reviews

UNION ALL

SELECT 
    'Duplicate product IDs in dim_products',
    COUNT(*) - COUNT(DISTINCT product_id),
    CASE WHEN COUNT(*) = COUNT(DISTINCT product_id) THEN 'PASS' ELSE 'FAIL' END
FROM dim_products;

-- =============================================================================
-- DATA RANGE VALIDATION
-- =============================================================================

SELECT '' as separator, 'DATA RANGE CHECK' as validation_type;

-- Check date ranges
SELECT 
    'Order date range validation' as check_description,
    MIN(order_purchase_timestamp) as earliest_order,
    MAX(order_purchase_timestamp) as latest_order,
    CASE 
        WHEN MIN(order_purchase_timestamp) >= '2016-01-01' 
         AND MAX(order_purchase_timestamp) <= '2019-12-31' 
        THEN 'PASS' 
        ELSE 'REVIEW' 
    END as status
FROM fact_orders;

-- Check geographic distribution
SELECT 
    'Customer geographic distribution' as check_description,
    customer_state,
    COUNT(*) as customer_count
FROM dim_customers 
GROUP BY customer_state 
ORDER BY customer_count DESC 
LIMIT 10;

-- Check product category distribution
SELECT 
    'Product category distribution' as check_description,
    product_category_name,
    COUNT(*) as product_count
FROM dim_products 
GROUP BY product_category_name 
ORDER BY product_count DESC 
LIMIT 10;

-- =============================================================================
-- DATA TRANSFORMATION VALIDATION
-- =============================================================================

SELECT '' as separator, 'DATA TRANSFORMATION CHECK' as validation_type;

-- Verify derived metrics calculations
SELECT 
    'Delivery metrics calculation check' as check_description,
    COUNT(*) as total_delivered_orders,
    COUNT(CASE WHEN delivery_days IS NOT NULL THEN 1 END) as orders_with_delivery_days,
    COUNT(CASE WHEN on_time_delivery = 1 THEN 1 END) as on_time_orders,
    COUNT(CASE WHEN on_time_delivery = 0 THEN 1 END) as late_orders
FROM fact_orders 
WHERE order_status = 'delivered';

-- Check for products with 'unknown' category (converted from NULL)
SELECT 
    'Products with unknown category (NULL conversion)' as check_description,
    COUNT(*) as unknown_category_products,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dim_products), 2) as percentage_unknown
FROM dim_products 
WHERE product_category_name = 'unknown';

-- =============================================================================
-- VIEW VALIDATION
-- =============================================================================

SELECT '' as separator, 'VIEW VALIDATION CHECK' as validation_type;

-- Test the order analysis view
SELECT 
    'Order analysis view test' as check_description,
    COUNT(*) as view_record_count,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM vw_order_analysis 
LIMIT 1;

-- Test the regional performance view
SELECT 
    'Regional performance view test' as check_description,
    COUNT(*) as view_record_count,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END as status
FROM vw_regional_performance 
LIMIT 1;

-- =============================================================================
-- SUMMARY STATISTICS
-- =============================================================================

SELECT '' as separator, 'DATA SUMMARY STATISTICS' as validation_type;

-- Overall data health summary
SELECT 
    'Database summary' as metric,
    (SELECT COUNT(*) FROM fact_orders) as total_orders,
    (SELECT COUNT(*) FROM dim_customers) as total_customers,
    (SELECT COUNT(*) FROM dim_products) as total_products,
    (SELECT COUNT(*) FROM dim_sellers) as total_sellers,
    (SELECT ROUND(AVG(delivery_days), 1) FROM fact_orders) as avg_delivery_days,
    (SELECT ROUND(AVG(on_time_delivery) * 100, 1) FROM fact_orders) as on_time_delivery_pct;

-- =============================================================================
-- VALIDATION COMPLETION
-- =============================================================================

SELECT '' as separator, 'VALIDATION COMPLETE' as validation_type;
SELECT 'Data validation script completed successfully' as status;
SELECT 'Review any FAIL or REVIEW statuses above for data quality issues' as note;
