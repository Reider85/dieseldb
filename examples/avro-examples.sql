-- AVRO Storage Examples for DieselDB
-- These examples demonstrate various SQL operations with AVRO storage format

-- =================================================================
-- Section 1: Basic Table Operations
-- =================================================================

-- Create a simple users table with AVRO storage
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255),
    age INT,
    created_at TIMESTAMP,
    is_active BOOLEAN,
    metadata MAP(STRING, STRING)
) STORED AS AVRO;

-- Create a products table with specific AVRO compression settings
CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(200),
    price DECIMAL(10,2),
    category VARCHAR(50),
    in_stock BOOLEAN,
    weight_kg DECIMAL(8,3),
    description TEXT,
    tags ARRAY(STRING),
    attributes MAP(STRING, STRING)
) STORED AS AVRO
WITH (
    'compression' = 'zstandard',
    'compression.level' = 5,
    'block.size' = 32768
);

-- Create a sales table with partitioning
CREATE TABLE sales (
    id INT,
    sale_date DATE,
    amount DECIMAL(12,2),
    customer_id INT,
    product_id INT,
    region VARCHAR(50),
    payment_method VARCHAR(20),
    tax_rate DECIMAL(5,4)
) STORED AS AVRO
PARTITIONED BY (region, sale_date);

-- =================================================================
-- Section 2: Data Insertion Examples
-- =================================================================

-- Insert single user
INSERT INTO users VALUES 
(1, 'Alice Johnson', 'alice@example.com', 30, '2023-01-15 10:00:00', true, '{"registration_source": "web", "preferences": "notifications"}');

-- Insert multiple users at once
INSERT INTO users VALUES 
(2, 'Bob Smith', 'bob@example.com', 25, '2023-01-16 11:30:00', true, '{"registration_source": "mobile", "preferences": "none"}'),
(3, 'Carol Davis', 'carol@example.com', 35, '2023-01-17 14:15:00', false, '{"registration_source": "web", "preferences": "email"}'),
(4, 'David Wilson', 'david@example.com', 28, '2023-01-18 09:45:00', true, '{"registration_source": "referral", "preferences": "sms"}');

-- Insert products with various data types
INSERT INTO products VALUES 
(1, 'Laptop Pro 15"', 1299.99, 'Electronics', true, 2.1, 'High-performance laptop with 16GB RAM', ARRAY('laptop', 'computer', 'professional'), '{"brand": "TechCorp", "warranty": "2 years", "color": "silver"}'),
(2, 'Wireless Mouse', 29.99, 'Electronics', true, 0.15, 'Ergonomic wireless mouse with precision tracking', ARRAY('mouse', 'wireless', 'input'), '{"brand": "TechCorp", "warranty": "1 year", "color": "black"}'),
(3, 'Mechanical Keyboard', 89.99, 'Electronics', false, 1.2, 'RGB backlit mechanical keyboard with blue switches', ARRAY('keyboard', 'mechanical', 'rgb'), '{"brand": "TechCorp", "warranty": "3 years", "color": "black"}'),
(4, 'Office Chair', 299.99, 'Furniture', true, 18.5, 'Ergonomic office chair with lumbar support', ARRAY('chair', 'furniture', 'ergonomic'), '{"brand": "ComfortMax", "warranty": "5 years", "color": "black"}'),
(5, 'Standing Desk', 449.99, 'Furniture', true, 25.0, 'Electric height-adjustable standing desk', ARRAY('desk', 'furniture', 'standing'), '{"brand": "ErgoDesk", "warranty": "3 years", "color": "white"}');

-- Insert sales data
INSERT INTO sales VALUES 
(1, '2023-01-15', 1299.99, 1, 1, 'North', 'credit_card', 0.0825),
(2, '2023-01-15', 29.99, 2, 2, 'South', 'paypal', 0.0650),
(3, '2023-01-16', 89.99, 3, 3, 'East', 'credit_card', 0.0725),
(4, '2023-01-16', 299.99, 4, 4, 'West', 'debit_card', 0.0800),
(5, '2023-01-17', 449.99, 1, 5, 'North', 'credit_card', 0.0825);

-- =================================================================
-- Section 3: Basic Query Examples
-- =================================================================

-- Select all users
SELECT * FROM users;

-- Select specific columns
SELECT id, name, email FROM users WHERE is_active = true;

-- Select with WHERE clause
SELECT * FROM users WHERE age > 30;

-- Select with LIKE pattern matching
SELECT * FROM users WHERE name LIKE '%J%';

-- Select with ORDER BY
SELECT * FROM users ORDER BY age DESC;

-- Select with LIMIT
SELECT * FROM users LIMIT 3;

-- Select with calculated columns
SELECT name, age, age + 5 as age_in_5_years FROM users;

-- =================================================================
-- Section 4: Advanced Query Examples
-- =================================================================

-- Aggregation functions
SELECT 
    category,
    COUNT(*) as product_count,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    SUM(price) as total_value
FROM products 
GROUP BY category;

-- Aggregation with HAVING
SELECT 
    region,
    COUNT(*) as sales_count,
    AVG(amount) as avg_sale_amount
FROM sales
GROUP BY region
HAVING COUNT(*) > 1;

-- Window functions
SELECT 
    name,
    price,
    category,
    AVG(price) OVER (PARTITION BY category) as category_avg,
    RANK() OVER (ORDER BY price DESC) as price_rank,
    ROW_NUMBER() OVER (ORDER BY created_at) as user_order
FROM products;

-- Subquery in WHERE clause
SELECT * FROM users 
WHERE age > (SELECT AVG(age) FROM users);

-- Subquery in SELECT clause
SELECT 
    u.name,
    u.age,
    (SELECT COUNT(*) FROM sales s WHERE s.customer_id = u.id) as purchase_count
FROM users u;

-- EXISTS with subquery
SELECT * FROM products p
WHERE EXISTS (SELECT 1 FROM sales s WHERE s.product_id = p.id);

-- IN with subquery
SELECT * FROM users 
WHERE id IN (SELECT customer_id FROM sales WHERE amount > 100);

-- =================================================================
-- Section 5: JOIN Examples
-- =================================================================

-- INNER JOIN
SELECT 
    u.name as customer_name,
    u.email,
    s.sale_date,
    s.amount,
    p.name as product_name
FROM users u
JOIN sales s ON u.id = s.customer_id
JOIN products p ON s.product_id = p.id;

-- LEFT JOIN
SELECT 
    u.name,
    u.email,
    COUNT(s.id) as purchase_count
FROM users u
LEFT JOIN sales s ON u.id = s.customer_id
GROUP BY u.id, u.name, u.email;

-- RIGHT JOIN (simulated with LEFT JOIN)
SELECT 
    p.name as product_name,
    p.category,
    COUNT(s.id) as sales_count
FROM products p
LEFT JOIN sales s ON p.id = s.product_id
GROUP BY p.id, p.name, p.category;

-- Multiple JOINs
SELECT 
    u.name as customer_name,
    p.name as product_name,
    s.sale_date,
    s.amount,
    s.region
FROM users u
JOIN sales s ON u.id = s.customer_id
JOIN products p ON s.product_id = p.id
WHERE s.region = 'North';

-- Self JOIN
SELECT 
    e.name as employee_name,
    m.name as manager_name
FROM users e
LEFT JOIN users m ON e.id = m.id; -- Note: This is a simplified example

-- =================================================================
-- Section 6: Complex Query Examples
-- =================================================================

-- Complex WHERE with multiple conditions
SELECT 
    u.name,
    p.name,
    s.sale_date,
    s.amount,
    CASE 
        WHEN s.amount > 500 THEN 'High Value'
        WHEN s.amount > 100 THEN 'Medium Value'
        ELSE 'Low Value'
    END as value_category
FROM users u
JOIN sales s ON u.id = s.customer_id
JOIN products p ON s.product_id = p.id
WHERE s.sale_date BETWEEN '2023-01-15' AND '2023-01-17'
  AND s.amount > 50
  AND p.category = 'Electronics'
ORDER BY s.amount DESC;

-- Common Table Expression (CTE)
WITH high_value_customers AS (
    SELECT 
        customer_id,
        COUNT(*) as purchase_count,
        SUM(amount) as total_spent
    FROM sales
    GROUP BY customer_id
    HAVING SUM(amount) > 1000
),
customer_details AS (
    SELECT * FROM users u
    JOIN high_value_customers h ON u.id = h.customer_id
)
SELECT 
    c.name,
    c.email,
    h.purchase_count,
    h.total_spent,
    p.name as favorite_product
FROM customer_details c
JOIN (
    SELECT 
        customer_id,
        product_id,
        COUNT(*) as purchase_count
    FROM sales
    GROUP BY customer_id, product_id
    ORDER BY purchase_count DESC
) f ON c.id = f.customer_id
JOIN products p ON f.product_id = p.id
WHERE f.purchase_count = (SELECT MAX(purchase_count) FROM (
    SELECT customer_id, COUNT(*) as purchase_count 
    FROM sales 
    GROUP BY customer_id
) t);

-- Pivot-like query
SELECT 
    product_name,
    SUM(CASE WHEN region = 'North' THEN amount ELSE 0 END) as north_sales,
    SUM(CASE WHEN region = 'South' THEN amount ELSE 0 END) as south_sales,
    SUM(CASE WHEN region = 'East' THEN amount ELSE 0 END) as east_sales,
    SUM(CASE WHEN region = 'West' THEN amount ELSE 0 END) as west_sales,
    SUM(amount) as total_sales
FROM (
    SELECT 
        p.name as product_name,
        s.region,
        s.amount
    FROM products p
    JOIN sales s ON p.id = s.product_id
) sales_data
GROUP BY product_name
ORDER BY total_sales DESC;

-- =================================================================
-- Section 7: Update and Delete Examples
-- =================================================================

-- Update single record
UPDATE users SET is_active = false WHERE id = 3;

-- Update multiple records
UPDATE products SET in_stock = false WHERE category = 'Electronics' AND price < 50;

-- Update with calculation
UPDATE sales SET amount = amount * 1.1 WHERE region = 'North';

-- Delete single record
DELETE FROM sales WHERE id = 5;

-- Delete with conditions
DELETE FROM sales WHERE sale_date < '2023-01-16' AND amount < 50;

-- =================================================================
-- Section 8: Transaction Examples
-- =================================================================

-- Simple transaction
BEGIN TRANSACTION;
INSERT INTO users VALUES (5, 'Eve Brown', 'eve@example.com', 32, '2023-01-19 16:20:00', true, '{"registration_source": "web"}');
INSERT INTO sales VALUES (6, '2023-01-19', 159.99, 5, 2, 'East', 'credit_card', 0.0725);
COMMIT;

-- Transaction with rollback
BEGIN TRANSACTION;
UPDATE users SET is_active = false WHERE id = 4;
-- Simulate error
-- INSERT INTO invalid_table VALUES (1);
ROLLBACK;

-- =================================================================
-- Section 9: Index Optimization Examples
-- =================================================================

-- Create indexes for better performance
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_sales_date ON sales(sale_date);
CREATE INDEX idx_sales_customer ON sales(customer_id);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_sales_region ON sales(region);

-- Use indexes in queries
SELECT * FROM sales WHERE sale_date = '2023-01-15' ORDER BY amount DESC;
SELECT * FROM users WHERE email LIKE '%@example.com';
SELECT * FROM products WHERE category = 'Electronics';

-- =================================================================
-- Section 10: Statistical Analysis Examples
-- =================================================================

-- Customer analytics
SELECT 
    u.name,
    u.age,
    COUNT(s.id) as total_purchases,
    AVG(s.amount) as avg_purchase_amount,
    MAX(s.amount) as max_purchase_amount,
    MIN(s.amount) as min_purchase_amount,
    SUM(s.amount) as total_spent
FROM users u
LEFT JOIN sales s ON u.id = s.customer_id
GROUP BY u.id, u.name, u.age
ORDER BY total_spent DESC;

-- Product performance analysis
SELECT 
    p.name,
    p.category,
    COUNT(s.id) as units_sold,
    SUM(s.amount) as revenue,
    AVG(s.amount) as avg_sale_price,
    COUNT(DISTINCT s.customer_id) as unique_customers
FROM products p
LEFT JOIN sales s ON p.id = s.product_id
GROUP BY p.id, p.name, p.category
ORDER BY revenue DESC;

-- Regional sales analysis
SELECT 
    region,
    COUNT(*) as transaction_count,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_transaction_value,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT product_id) as unique_products
FROM sales
GROUP BY region
ORDER BY total_revenue DESC;

-- Time series analysis
SELECT 
    sale_date,
    COUNT(*) as daily_transactions,
    SUM(amount) as daily_revenue,
    AVG(amount) as avg_transaction_value,
    COUNT(DISTINCT customer_id) as daily_customers
FROM sales
GROUP BY sale_date
ORDER BY sale_date;

-- =================================================================
-- Section 11: Data Validation Examples
-- =================================================================

-- Check data consistency
SELECT 
    p.name as product,
    COUNT(s.id) as sales_count,
    SUM(s.amount) as total_revenue
FROM products p
LEFT JOIN sales s ON p.id = s.product_id
GROUP BY p.id, p.name
HAVING COUNT(s.id) = 0;  -- Products with no sales

-- Find duplicate emails
SELECT email, COUNT(*) as duplicate_count
FROM users
GROUP BY email
HAVING COUNT(*) > 1;

-- Validate data ranges
SELECT * FROM users WHERE age < 0 OR age > 150;
SELECT * FROM sales WHERE amount <= 0;
SELECT * FROM products WHERE price < 0;

-- =================================================================
-- Section 12: Cleanup Examples
-- =================================================================

-- Drop indexes
DROP INDEX idx_users_email;
DROP INDEX idx_sales_date;
DROP INDEX idx_sales_customer;
DROP INDEX idx_products_category;
DROP INDEX idx_sales_region;

-- Drop tables (in reverse order of dependencies)
DROP TABLE sales;
DROP TABLE products;
DROP TABLE users;

-- =================================================================
-- End of Examples
-- =================================================================

-- These examples demonstrate the full capabilities of AVRO storage in DieselDB
-- including basic operations, complex queries, joins, transactions, and analytics