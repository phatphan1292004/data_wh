-- Database: movie_staging

CREATE DATABASE IF NOT EXISTS movie_staging CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE movie_staging;

-- Bảng lưu raw data từ crawler
CREATE TABLE IF NOT EXISTS raw_movies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    raw_data JSON NOT NULL,
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE,
    INDEX idx_source (source),
    INDEX idx_crawled_at (crawled_at),
    INDEX idx_processed (processed)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Bảng staging sau khi deduplicate
CREATE TABLE IF NOT EXISTS staging_movies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    raw_id INT,
    title VARCHAR(500),
    tmdb_id VARCHAR(50),
    detail_url TEXT,
    status VARCHAR(100),
    category VARCHAR(100),
    total_episodes INT,
    duration VARCHAR(100),
    release_year INT,
    quality VARCHAR(50),
    language VARCHAR(100),
    director TEXT,
    actors TEXT,
    genre TEXT,
    origin_country TEXT,
    poster TEXT,
    description TEXT,
    episodes JSON,
    updated_at VARCHAR(100),
    crawled_at TIMESTAMP NULL DEFAULT NULL,
    is_duplicate BOOLEAN DEFAULT FALSE,
    duplicate_of INT,
    processing_step VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (raw_id) REFERENCES raw_movies(id),
    INDEX idx_tmdb_id (tmdb_id),
    INDEX idx_title (title(255)),
    INDEX idx_processing_step (processing_step)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Bảng lưu lỗi validation
CREATE TABLE IF NOT EXISTS validation_errors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    staging_id INT,
    error_type VARCHAR(100),
    field_name VARCHAR(100),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (staging_id) REFERENCES staging_movies(id),
    INDEX idx_error_type (error_type),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Bảng log xử lý
CREATE TABLE IF NOT EXISTS processing_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(100),
    step_name VARCHAR(100),
    status VARCHAR(50),
    records_processed INT,
    records_success INT,
    records_failed INT,
    start_time TIMESTAMP NULL DEFAULT NULL,
    end_time TIMESTAMP NULL DEFAULT NULL,
    error_message TEXT,
    INDEX idx_batch_id (batch_id),
    INDEX idx_step_name (step_name),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
