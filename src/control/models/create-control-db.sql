-- Database: movie_control
-- Purpose: Centralized logging and control for ETL processes

CREATE DATABASE IF NOT EXISTS movie_control 
  CHARACTER SET utf8mb4 
  COLLATE utf8mb4_unicode_ci;

USE movie_control;

-- Bảng log xử lý ETL
CREATE TABLE IF NOT EXISTS processing_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(100) UNIQUE NOT NULL,
    step_name VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'running',
    records_processed INT DEFAULT 0,
    records_success INT DEFAULT 0,
    records_failed INT DEFAULT 0,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP NULL,
    error_message TEXT,
    INDEX idx_batch_id (batch_id),
    INDEX idx_step_name (step_name),
    INDEX idx_status (status),
    INDEX idx_start_time (start_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
