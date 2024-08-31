-- Создание таблицы организаций
CREATE TABLE organizations (
    org_id SERIAL PRIMARY KEY,
    inn VARCHAR(20) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL
);

-- Создание таблицы транзакций
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    org_id INTEGER NOT NULL,
    amount DECIMAL(20, 2) NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    currency_code VARCHAR(10) NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    FOREIGN KEY (org_id) REFERENCES organizations(org_id)
);