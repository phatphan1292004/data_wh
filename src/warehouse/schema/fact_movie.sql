USE movie_dwh;

CREATE TABLE IF NOT EXISTS fact_movie (
    movie_key INT AUTO_INCREMENT PRIMARY KEY,
    
    -- Business Key
    tmdb_id VARCHAR(50),
    source VARCHAR(50) NOT NULL,
    
    -- Movie Information
    title VARCHAR(500) NOT NULL,
    original_title VARCHAR(500),
    description TEXT,
    poster_url TEXT,
    detail_url TEXT,
    
    -- Metrics
    total_episodes INT,
    duration_minutes INT,
    quality VARCHAR(50),
    language VARCHAR(100),
    status VARCHAR(100),
    category VARCHAR(100),
    
    -- Date Dimension Foreign Keys
    release_date_key INT,
    crawled_date_key INT,
    updated_date_key INT,
    
    -- Temporal Information
    release_year INT,
    crawled_at TIMESTAMP NULL DEFAULT NULL,
    updated_at TIMESTAMP NULL DEFAULT NULL,
    
    -- Episode Data
    episodes JSON,
    
    -- SCD Type 2 Fields
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP NULL DEFAULT NULL,
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uk_tmdb_source_current (tmdb_id, source, is_current),
    INDEX idx_title (title(255)),
    INDEX idx_release_year (release_year),
    INDEX idx_status (status),
    INDEX idx_category (category),
    INDEX idx_is_current (is_current),
    FOREIGN KEY (release_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (crawled_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (updated_date_key) REFERENCES dim_date(date_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Bridge table: Movie - Genre (Many-to-Many)
CREATE TABLE IF NOT EXISTS bridge_movie_genre (
    movie_key INT NOT NULL,
    genre_key INT NOT NULL,
    PRIMARY KEY (movie_key, genre_key),
    FOREIGN KEY (movie_key) REFERENCES fact_movie(movie_key) ON DELETE CASCADE,
    FOREIGN KEY (genre_key) REFERENCES dim_genre(genre_key) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Bridge table: Movie - Country (Many-to-Many)
CREATE TABLE IF NOT EXISTS bridge_movie_country (
    movie_key INT NOT NULL,
    country_key INT NOT NULL,
    PRIMARY KEY (movie_key, country_key),
    FOREIGN KEY (movie_key) REFERENCES fact_movie(movie_key) ON DELETE CASCADE,
    FOREIGN KEY (country_key) REFERENCES dim_country(country_key) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Bridge table: Movie - Person (Many-to-Many with role)
CREATE TABLE IF NOT EXISTS bridge_movie_person (
    movie_key INT NOT NULL,
    person_key INT NOT NULL,
    role_type ENUM('director', 'actor') NOT NULL,
    PRIMARY KEY (movie_key, person_key, role_type),
    FOREIGN KEY (movie_key) REFERENCES fact_movie(movie_key) ON DELETE CASCADE,
    FOREIGN KEY (person_key) REFERENCES dim_person(person_key) ON DELETE CASCADE,
    INDEX idx_role_type (role_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
