-- =============================================================================
-- PostgreSQL blackbox-test seed data
--
-- Creates three user databases (sales, hr, analytics) with diverse schemas,
-- data types, roles, and realistic row counts (100+ rows per table).
--
-- Executed as postgres superuser via /docker-entrypoint-initdb.d/
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Global roles (created at cluster level, before any \c switch)
-- ---------------------------------------------------------------------------
CREATE ROLE readonly_role  NOLOGIN;
CREATE ROLE readwrite_role NOLOGIN;
CREATE ROLE admin_role     NOLOGIN SUPERUSER;

-- Per-database reader/writer login users (used in permission extraction tests)
CREATE ROLE sales_reader   LOGIN PASSWORD 'pass';
CREATE ROLE sales_writer   LOGIN PASSWORD 'pass';
CREATE ROLE hr_reader      LOGIN PASSWORD 'pass';
CREATE ROLE hr_writer      LOGIN PASSWORD 'pass';
CREATE ROLE analytics_reader LOGIN PASSWORD 'pass';
CREATE ROLE analytics_writer LOGIN PASSWORD 'pass';

-- Connector's own login (used in credentials).
-- Use DO block because testcontainers sets POSTGRES_USER=connector_user, which
-- pre-creates the role before init scripts run.
DO $$
BEGIN
    CREATE ROLE connector_user LOGIN PASSWORD 'connector_pass' SUPERUSER;
EXCEPTION WHEN duplicate_object THEN NULL;
END
$$;

-- ---------------------------------------------------------------------------
-- Database: sales
-- ---------------------------------------------------------------------------
CREATE DATABASE sales;

\c sales

-- Grants
GRANT CONNECT ON DATABASE sales TO sales_reader, sales_writer, connector_user;
GRANT USAGE ON SCHEMA public TO sales_reader, sales_writer, connector_user;

-- ---- customers ----
CREATE TABLE customers (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    first_name    VARCHAR(80) NOT NULL,
    last_name     VARCHAR(80) NOT NULL,
    email         VARCHAR(150) UNIQUE NOT NULL,
    phone         VARCHAR(30),
    date_of_birth DATE,
    account_balance NUMERIC(12,2) DEFAULT 0.00,
    credit_limit  NUMERIC(12,2) DEFAULT 5000.00,
    is_active     BOOLEAN DEFAULT TRUE,
    metadata      JSONB,
    notes         TEXT,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO customers (first_name, last_name, email, phone, date_of_birth,
                        account_balance, credit_limit, is_active, metadata, notes)
SELECT
    'First' || i,
    'Last'  || i,
    'user'  || i || '@sales.example.com',
    '+1-555-' || lpad(i::text, 7, '0'),
    '1980-01-01'::date + (i || ' days')::interval,
    (random() * 10000)::numeric(12,2),
    5000.00 + (random() * 5000)::numeric(12,2),
    (i % 7 != 0),
    jsonb_build_object('tier', CASE i%3 WHEN 0 THEN 'gold' WHEN 1 THEN 'silver' ELSE 'bronze' END),
    'Note for customer ' || i
FROM generate_series(1, 200) AS i;

GRANT SELECT ON customers TO sales_reader;
GRANT SELECT, INSERT, UPDATE ON customers TO sales_writer;

-- ---- products ----
CREATE TABLE products (
    id            SERIAL PRIMARY KEY,
    sku           VARCHAR(50) UNIQUE NOT NULL,
    name          VARCHAR(200) NOT NULL,
    description   TEXT,
    category      VARCHAR(80),
    price         NUMERIC(10,2) NOT NULL,
    cost          NUMERIC(10,2),
    weight_kg     NUMERIC(6,3),
    in_stock      BOOLEAN DEFAULT TRUE,
    stock_count   INT DEFAULT 0,
    image_data    BYTEA,
    attributes    JSONB,
    created_at    TIMESTAMP DEFAULT NOW()
);

INSERT INTO products (sku, name, description, category, price, cost,
                       weight_kg, in_stock, stock_count, attributes)
SELECT
    'SKU-' || lpad(i::text, 6, '0'),
    'Product ' || i,
    'Description of product ' || i,
    CASE i%5 WHEN 0 THEN 'Electronics' WHEN 1 THEN 'Clothing'
             WHEN 2 THEN 'Food' WHEN 3 THEN 'Books' ELSE 'Other' END,
    (5 + random() * 500)::numeric(10,2),
    (2 + random() * 200)::numeric(10,2),
    (0.1 + random() * 10)::numeric(6,3),
    (i % 10 != 0),
    (random() * 500)::int,
    jsonb_build_object('color', CASE i%4 WHEN 0 THEN 'red' WHEN 1 THEN 'blue'
                                          WHEN 2 THEN 'green' ELSE 'black' END)
FROM generate_series(1, 150) AS i;

-- Put a small binary blob in image_data for a few rows (tests BYTEA sampling)
UPDATE products SET image_data = decode('89504e47', 'hex') WHERE id <= 10;

GRANT SELECT ON products TO sales_reader;
GRANT SELECT, INSERT, UPDATE ON products TO sales_writer;

-- ---- orders ----
CREATE TABLE orders (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id   UUID        NOT NULL REFERENCES customers(id),
    order_date    TIMESTAMP   NOT NULL DEFAULT NOW(),
    shipped_at    TIMESTAMP,
    status        VARCHAR(30) NOT NULL DEFAULT 'pending',
    total_amount  NUMERIC(12,2) NOT NULL DEFAULT 0.00,
    currency      VARCHAR(3)  DEFAULT 'USD',
    shipping_addr JSONB,
    notes         TEXT
);

INSERT INTO orders (customer_id, order_date, status, total_amount, currency, shipping_addr)
SELECT
    (SELECT id FROM customers ORDER BY random() LIMIT 1),
    NOW() - (random() * 365 || ' days')::interval,
    CASE (random()*4)::int WHEN 0 THEN 'pending' WHEN 1 THEN 'processing'
                           WHEN 2 THEN 'shipped'  ELSE 'delivered' END,
    (10 + random() * 2000)::numeric(12,2),
    'USD',
    jsonb_build_object('street', i || ' Main St', 'city', 'Testville', 'zip', '10001')
FROM generate_series(1, 300) AS i;

GRANT SELECT ON orders TO sales_reader;
GRANT SELECT, INSERT, UPDATE ON orders TO sales_writer;

-- ---- order_items ----
CREATE TABLE order_items (
    id          SERIAL PRIMARY KEY,
    order_id    UUID   NOT NULL REFERENCES orders(id),
    product_id  INT    NOT NULL REFERENCES products(id),
    quantity    INT    NOT NULL DEFAULT 1,
    unit_price  NUMERIC(10,2) NOT NULL,
    discount    NUMERIC(5,2)  DEFAULT 0.00,
    line_total  NUMERIC(12,2) GENERATED ALWAYS AS (quantity * unit_price * (1 - discount/100)) STORED
);

INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount)
SELECT
    (SELECT id FROM orders ORDER BY random() LIMIT 1),
    1 + (random() * 149)::int,
    1 + (random() * 5)::int,
    (5 + random() * 200)::numeric(10,2),
    (random() * 20)::numeric(5,2)
FROM generate_series(1, 500) AS i;

GRANT SELECT ON order_items TO sales_reader;
GRANT SELECT, INSERT, UPDATE ON order_items TO sales_writer;

-- ---- VIEW: active_customers ----
CREATE VIEW active_customers AS
SELECT id, first_name, last_name, email, account_balance
FROM customers
WHERE is_active = TRUE;

GRANT SELECT ON active_customers TO sales_reader;

-- ---- VIEW: monthly_sales ----
CREATE VIEW monthly_sales AS
SELECT
    date_trunc('month', order_date) AS month,
    COUNT(*)                        AS order_count,
    SUM(total_amount)               AS revenue
FROM orders
GROUP BY 1
ORDER BY 1 DESC;

GRANT SELECT ON monthly_sales TO sales_reader;


-- ---------------------------------------------------------------------------
-- Database: hr
-- ---------------------------------------------------------------------------

\c postgres
CREATE DATABASE hr;

\c hr

GRANT CONNECT ON DATABASE hr TO hr_reader, hr_writer, connector_user;
GRANT USAGE ON SCHEMA public TO hr_reader, hr_writer, connector_user;

-- ---- departments ----
CREATE TABLE departments (
    id          SERIAL      PRIMARY KEY,
    name        VARCHAR(100) NOT NULL UNIQUE,
    code        VARCHAR(10)  NOT NULL UNIQUE,
    location    VARCHAR(100),
    budget      NUMERIC(15,2),
    head_count  INT DEFAULT 0,
    created_at  TIMESTAMP DEFAULT NOW()
);

INSERT INTO departments (name, code, location, budget, head_count)
VALUES
    ('Engineering',     'ENG',  'San Francisco', 2500000.00, 45),
    ('Sales',           'SLS',  'New York',       1800000.00, 30),
    ('Human Resources', 'HR',   'Chicago',         600000.00, 12),
    ('Marketing',       'MKT',  'Los Angeles',    1200000.00, 20),
    ('Finance',         'FIN',  'New York',        900000.00, 15),
    ('Operations',      'OPS',  'Dallas',         1100000.00, 25),
    ('Legal',           'LEG',  'Washington DC',   500000.00,  8),
    ('Research',        'RES',  'Boston',         3000000.00, 35);

GRANT SELECT ON departments TO hr_reader;
GRANT SELECT, INSERT, UPDATE ON departments TO hr_writer;

-- ---- job_titles ----
CREATE TABLE job_titles (
    id              SERIAL      PRIMARY KEY,
    title           VARCHAR(100) NOT NULL UNIQUE,
    grade           VARCHAR(10),
    min_salary      NUMERIC(10,2),
    max_salary      NUMERIC(10,2),
    is_management   BOOLEAN DEFAULT FALSE
);

INSERT INTO job_titles (title, grade, min_salary, max_salary, is_management)
VALUES
    ('Software Engineer I',     'L3',  80000,  120000, FALSE),
    ('Software Engineer II',    'L4', 110000,  160000, FALSE),
    ('Senior Software Engineer','L5', 140000,  200000, FALSE),
    ('Staff Engineer',          'L6', 180000,  250000, FALSE),
    ('Principal Engineer',      'L7', 220000,  300000, FALSE),
    ('Engineering Manager',     'M4', 180000,  250000, TRUE),
    ('Senior Manager',          'M5', 220000,  300000, TRUE),
    ('Director of Engineering', 'M6', 280000,  380000, TRUE),
    ('VP of Engineering',       'E1', 350000,  500000, TRUE),
    ('Product Manager',         'M4', 150000,  210000, FALSE),
    ('Data Scientist',          'L4', 120000,  180000, FALSE),
    ('DevOps Engineer',         'L4', 110000,  160000, FALSE),
    ('UX Designer',             'L3',  90000,  140000, FALSE),
    ('Sales Representative',    'S2',  60000,  100000, FALSE),
    ('Account Executive',       'S3',  80000,  130000, FALSE);

GRANT SELECT ON job_titles TO hr_reader;
GRANT SELECT, INSERT, UPDATE ON job_titles TO hr_writer;

-- ---- employees ----
CREATE TABLE employees (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    employee_number VARCHAR(20) UNIQUE NOT NULL,
    first_name      VARCHAR(80) NOT NULL,
    last_name       VARCHAR(80) NOT NULL,
    email           VARCHAR(150) UNIQUE NOT NULL,
    phone           VARCHAR(30),
    hire_date       DATE        NOT NULL,
    termination_date DATE,
    department_id   INT         REFERENCES departments(id),
    job_title_id    INT         REFERENCES job_titles(id),
    manager_id      UUID        REFERENCES employees(id),
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    nationality     VARCHAR(50),
    tax_id          VARCHAR(50),
    ssn_hash        BYTEA,      -- hashed for privacy; tests BYTEA type
    metadata        JSONB,
    notes           TEXT,
    created_at      TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- Seed 200 employees
INSERT INTO employees (employee_number, first_name, last_name, email,
                        hire_date, department_id, job_title_id, is_active,
                        nationality, metadata)
SELECT
    'EMP-' || lpad(i::text, 5, '0'),
    'Fname' || i,
    'Lname' || i,
    'emp' || i || '@company.hr',
    '2010-01-01'::date + (i * 3 || ' days')::interval,
    1 + (i % 8),
    1 + (i % 15),
    (i % 20 != 0),
    CASE i%5 WHEN 0 THEN 'US' WHEN 1 THEN 'GB'
             WHEN 2 THEN 'DE' WHEN 3 THEN 'IN' ELSE 'CA' END,
    jsonb_build_object('remote', i%3=0, 'floor', (i%5)+1)
FROM generate_series(1, 200) AS i;

-- pgcrypto is required for digest() below
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Add SSN hash bytes for first 50 employees
UPDATE employees SET ssn_hash = digest(employee_number, 'sha256')
WHERE ctid IN (SELECT ctid FROM employees LIMIT 50);

GRANT SELECT ON employees TO hr_reader;
GRANT SELECT, INSERT, UPDATE ON employees TO hr_writer;

-- ---- salaries ----
CREATE TABLE salaries (
    id            SERIAL      PRIMARY KEY,
    employee_id   UUID        NOT NULL REFERENCES employees(id),
    base_salary   NUMERIC(12,2) NOT NULL,
    bonus         NUMERIC(12,2) DEFAULT 0,
    currency      VARCHAR(3)  DEFAULT 'USD',
    effective_from DATE       NOT NULL,
    effective_to  DATE,
    pay_grade     VARCHAR(10),
    is_current    BOOLEAN     DEFAULT TRUE
);

INSERT INTO salaries (employee_id, base_salary, bonus, effective_from, pay_grade, is_current)
SELECT
    id,
    80000 + (random() * 200000)::numeric(12,2),
    (random() * 20000)::numeric(12,2),
    hire_date,
    CASE job_title_id%5
        WHEN 0 THEN 'L3' WHEN 1 THEN 'L4' WHEN 2 THEN 'L5'
        WHEN 3 THEN 'M4' ELSE 'L6' END,
    TRUE
FROM employees;

GRANT SELECT ON salaries TO hr_reader;
GRANT SELECT, INSERT, UPDATE ON salaries TO hr_writer;

-- ---- VIEW: current_employees ----
CREATE VIEW current_employees AS
SELECT
    e.id,
    e.employee_number,
    e.first_name,
    e.last_name,
    e.email,
    d.name   AS department,
    jt.title AS job_title
FROM employees e
JOIN departments  d  ON d.id  = e.department_id
JOIN job_titles   jt ON jt.id = e.job_title_id
WHERE e.is_active = TRUE;

GRANT SELECT ON current_employees TO hr_reader;

-- ---- VIEW: salary_summary ----
CREATE VIEW salary_summary AS
SELECT
    d.name          AS department,
    COUNT(e.id)     AS employee_count,
    AVG(s.base_salary)::numeric(12,2) AS avg_salary,
    MAX(s.base_salary)                AS max_salary,
    MIN(s.base_salary)                AS min_salary
FROM employees e
JOIN departments d ON d.id = e.department_id
JOIN salaries    s ON s.employee_id = e.id AND s.is_current = TRUE
GROUP BY d.name;

GRANT SELECT ON salary_summary TO hr_reader;


-- ---------------------------------------------------------------------------
-- Database: analytics
-- ---------------------------------------------------------------------------

\c postgres
CREATE DATABASE analytics;

\c analytics

GRANT CONNECT ON DATABASE analytics TO analytics_reader, analytics_writer, connector_user;
GRANT USAGE ON SCHEMA public TO analytics_reader, analytics_writer, connector_user;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ---- sessions ----
CREATE TABLE sessions (
    id             UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id        UUID,
    session_token  VARCHAR(128) UNIQUE NOT NULL,
    ip_address     VARCHAR(45),        -- IPv4 or IPv6
    user_agent     TEXT,
    started_at     TIMESTAMP   NOT NULL DEFAULT NOW(),
    ended_at       TIMESTAMP,
    duration_secs  INT,
    is_mobile      BOOLEAN     DEFAULT FALSE,
    country_code   VARCHAR(2),
    referrer       TEXT,
    attributes     JSONB
);

INSERT INTO sessions (user_id, session_token, ip_address, user_agent,
                       started_at, duration_secs, is_mobile, country_code, attributes)
SELECT
    uuid_generate_v4(),
    md5(random()::text),
    (1+random()*254)::int || '.' || (1+random()*254)::int || '.' ||
    (1+random()*254)::int || '.' || (1+random()*254)::int,
    CASE i%3 WHEN 0 THEN 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
             WHEN 1 THEN 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0)'
             ELSE          'Mozilla/5.0 (Macintosh; Intel Mac OS X)' END,
    NOW() - (random() * 90 || ' days')::interval,
    (60 + random() * 3600)::int,
    (i % 3 = 1),
    CASE i%4 WHEN 0 THEN 'US' WHEN 1 THEN 'GB' WHEN 2 THEN 'DE' ELSE 'JP' END,
    jsonb_build_object('channel', CASE i%3 WHEN 0 THEN 'organic'
                                           WHEN 1 THEN 'paid' ELSE 'social' END)
FROM generate_series(1, 500) AS i;

GRANT SELECT ON sessions TO analytics_reader;
GRANT SELECT, INSERT, UPDATE ON sessions TO analytics_writer;

-- ---- events ----
CREATE TABLE events (
    id            BIGSERIAL   PRIMARY KEY,
    session_id    UUID        REFERENCES sessions(id),
    event_type    VARCHAR(80) NOT NULL,
    page_url      TEXT,
    element_id    VARCHAR(100),
    occurred_at   TIMESTAMP   NOT NULL DEFAULT NOW(),
    value         NUMERIC(12,4),
    label         VARCHAR(200),
    custom_data   JSONB,
    processing_ms INT
);

INSERT INTO events (session_id, event_type, page_url, occurred_at, value, label, custom_data)
SELECT
    (SELECT id FROM sessions ORDER BY random() LIMIT 1),
    CASE i%6 WHEN 0 THEN 'page_view' WHEN 1 THEN 'click'
             WHEN 2 THEN 'form_submit' WHEN 3 THEN 'purchase'
             WHEN 4 THEN 'signup' ELSE 'logout' END,
    '/page/' || (i % 20),
    NOW() - (random() * 90 || ' days')::interval,
    CASE WHEN i%6 = 3 THEN (5 + random() * 500)::numeric(12,4) ELSE NULL END,
    'Event label ' || i,
    jsonb_build_object('variant', CASE i%2 WHEN 0 THEN 'A' ELSE 'B' END)
FROM generate_series(1, 1000) AS i;

GRANT SELECT ON events TO analytics_reader;
GRANT SELECT, INSERT, UPDATE ON events TO analytics_writer;

-- ---- metrics ----
CREATE TABLE metrics (
    id            BIGSERIAL   PRIMARY KEY,
    metric_name   VARCHAR(100) NOT NULL,
    metric_value  NUMERIC(18,6) NOT NULL,
    tags          JSONB,
    recorded_at   TIMESTAMP   NOT NULL DEFAULT NOW(),
    period        VARCHAR(20),   -- hourly, daily, weekly
    dimensions    TEXT[]
);

INSERT INTO metrics (metric_name, metric_value, tags, recorded_at, period)
SELECT
    CASE i%5 WHEN 0 THEN 'page_views' WHEN 1 THEN 'unique_users'
             WHEN 2 THEN 'revenue'     WHEN 3 THEN 'conversion_rate'
             ELSE 'bounce_rate' END,
    (random() * 10000)::numeric(18,6),
    jsonb_build_object('env', CASE i%2 WHEN 0 THEN 'prod' ELSE 'staging' END),
    NOW() - (i || ' hours')::interval,
    CASE i%3 WHEN 0 THEN 'hourly' WHEN 1 THEN 'daily' ELSE 'weekly' END
FROM generate_series(1, 300) AS i;

GRANT SELECT ON metrics TO analytics_reader;
GRANT SELECT, INSERT, UPDATE ON metrics TO analytics_writer;

-- ---- feature_flags ----
CREATE TABLE feature_flags (
    id            SERIAL      PRIMARY KEY,
    flag_key      VARCHAR(100) NOT NULL UNIQUE,
    description   TEXT,
    is_enabled    BOOLEAN     NOT NULL DEFAULT FALSE,
    rollout_pct   NUMERIC(5,2) DEFAULT 0.00,  -- 0.00–100.00
    target_users  JSONB,
    created_by    VARCHAR(80),
    created_at    TIMESTAMP   DEFAULT NOW(),
    updated_at    TIMESTAMP   DEFAULT NOW()
);

INSERT INTO feature_flags (flag_key, description, is_enabled, rollout_pct, created_by)
SELECT
    'flag_' || i,
    'Feature flag number ' || i,
    (i % 3 != 0),
    (random() * 100)::numeric(5,2),
    'admin@company.com'
FROM generate_series(1, 50) AS i;

GRANT SELECT ON feature_flags TO analytics_reader;
GRANT SELECT, INSERT, UPDATE ON feature_flags TO analytics_writer;

-- ---------------------------------------------------------------------------
-- Schema: reporting (non-public schema for multi-schema discovery tests)
-- ---------------------------------------------------------------------------
CREATE SCHEMA reporting;
GRANT USAGE ON SCHEMA reporting TO analytics_reader, analytics_writer, connector_user;

-- ---- reporting.kpi_summary ----
CREATE TABLE reporting.kpi_summary (
    id            SERIAL      PRIMARY KEY,
    kpi_name      VARCHAR(100) NOT NULL,
    kpi_value     NUMERIC(18,4) NOT NULL,
    period        VARCHAR(20)  NOT NULL,
    recorded_at   TIMESTAMP    NOT NULL DEFAULT NOW()
);

INSERT INTO reporting.kpi_summary (kpi_name, kpi_value, period, recorded_at)
SELECT
    CASE i%4 WHEN 0 THEN 'revenue' WHEN 1 THEN 'churn_rate'
             WHEN 2 THEN 'nps_score' ELSE 'arpu' END,
    (random() * 10000)::numeric(18,4),
    CASE i%3 WHEN 0 THEN 'monthly' WHEN 1 THEN 'quarterly' ELSE 'yearly' END,
    NOW() - (i || ' days')::interval
FROM generate_series(1, 100) AS i;

GRANT SELECT ON reporting.kpi_summary TO analytics_reader;
GRANT SELECT, INSERT, UPDATE ON reporting.kpi_summary TO analytics_writer;

-- ---- VIEW: reporting.quarterly_kpis ----
CREATE VIEW reporting.quarterly_kpis AS
SELECT
    kpi_name,
    AVG(kpi_value)::numeric(18,4) AS avg_value,
    MAX(kpi_value)                AS max_value,
    COUNT(*)                      AS data_points
FROM reporting.kpi_summary
WHERE period = 'quarterly'
GROUP BY kpi_name;

GRANT SELECT ON reporting.quarterly_kpis TO analytics_reader;

-- ---- VIEW: daily_metrics ----
CREATE VIEW daily_metrics AS
SELECT
    date_trunc('day', recorded_at) AS day,
    metric_name,
    AVG(metric_value)::numeric(18,6) AS avg_value,
    MAX(metric_value)                AS max_value,
    COUNT(*)                         AS data_points
FROM metrics
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

GRANT SELECT ON daily_metrics TO analytics_reader;

-- ---- VIEW: user_funnel ----
CREATE VIEW user_funnel AS
SELECT
    event_type,
    COUNT(DISTINCT session_id) AS unique_sessions,
    COUNT(*)                   AS total_events,
    AVG(value)::numeric(12,4)  AS avg_value
FROM events
GROUP BY event_type
ORDER BY total_events DESC;

GRANT SELECT ON user_funnel TO analytics_reader;
