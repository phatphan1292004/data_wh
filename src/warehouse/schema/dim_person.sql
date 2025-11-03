USE movie_dwh;

CREATE TABLE IF NOT EXISTS dim_person (
    person_key INT AUTO_INCREMENT PRIMARY KEY,
    person_name VARCHAR(200) NOT NULL,
    person_type ENUM('director', 'actor', 'both') NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_person_name_type (person_name, person_type),
    INDEX idx_person_name (person_name),
    INDEX idx_person_type (person_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
