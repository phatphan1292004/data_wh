-- Database: movie_dwh

CREATE DATABASE IF NOT EXISTS movie_dwh CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE movie_dwh;

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20),
    week INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    INDEX idx_full_date (full_date),
    INDEX idx_year_month (year, month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
