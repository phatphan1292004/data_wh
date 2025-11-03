require('dotenv').config();
const mysql = require('mysql2/promise');
const fs = require('fs').promises;
const path = require('path');
const logger = require('../../../utils/logger');

async function deduplicateMovies() {
  const connection = await mysql.createConnection({
    host: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD,
    database: process.env.STAGING_DB_NAME || 'movie_staging'
  });

  try {
    // 1. Load raw data từ file JSON mới nhất
    const rawDir = path.join(__dirname, '../../../data/raw');
    const files = await fs.readdir(rawDir);
    const jsonFiles = files.filter(f => f.endsWith('.json')).sort().reverse();
    
    if (jsonFiles.length === 0) {
      throw new Error('No raw data files found');
    }

    const latestFile = path.join(rawDir, jsonFiles[0]);
    logger.info(`Loading data from: ${jsonFiles[0]}`);
    
    const rawData = JSON.parse(await fs.readFile(latestFile, 'utf8'));
    
    // 2. Insert vào raw_movies
    for (const movie of rawData.data) {
      await connection.query(
        `INSERT INTO raw_movies (source, raw_data, crawled_at, processed) 
         VALUES (?, ?, ?, FALSE)`,
        [rawData.metadata.source, JSON.stringify(movie), movie.crawledAt]
      );
    }
    
    logger.info(`Inserted ${rawData.data.length} records into raw_movies`);

    // 3. Load vào staging_movies (chưa deduplicate)
    const [rawMovies] = await connection.query(
      'SELECT * FROM raw_movies WHERE processed = FALSE'
    );

    let insertedCount = 0;
    for (const raw of rawMovies) {
      const movie = JSON.parse(raw.raw_data);
      
      await connection.query(
        `INSERT INTO staging_movies (
          raw_id, title, tmdb_id, detail_url, status, category,
          total_episodes, duration, release_year, quality, language,
          director, actors, genre, origin_country, poster, description,
          episodes, updated_at, crawled_at, processing_step
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'deduplicate')`,
        [
          raw.id,
          movie.title,
          movie.tmdbId,
          movie.detailUrl,
          movie.status,
          movie.category,
          parseInt(movie.totalEpisodes) || null,
          movie.duration,
          parseInt(movie.releaseYear) || null,
          movie.quality,
          movie.language,
          movie.director,
          movie.actors,
          movie.genre,
          movie.originCountry,
          movie.poster,
          movie.description,
          JSON.stringify(movie.episodes),
          movie.updatedAt,
          movie.crawledAt
        ]
      );
      insertedCount++;
    }

    logger.info(`Inserted ${insertedCount} records into staging_movies`);

    // 4. Đánh dấu duplicates dựa trên tmdb_id
    await connection.query(`
      UPDATE staging_movies s1
      JOIN (
        SELECT tmdb_id, MIN(id) as first_id
        FROM staging_movies
        WHERE tmdb_id IS NOT NULL
        GROUP BY tmdb_id
        HAVING COUNT(*) > 1
      ) s2 ON s1.tmdb_id = s2.tmdb_id
      SET s1.is_duplicate = TRUE,
          s1.duplicate_of = s2.first_id
      WHERE s1.id != s2.first_id
    `);

    const [duplicates] = await connection.query(
      'SELECT COUNT(*) as count FROM staging_movies WHERE is_duplicate = TRUE'
    );

    logger.info(`Found ${duplicates[0].count} duplicate records`);

    // 5. Đánh dấu raw_movies đã xử lý
    await connection.query('UPDATE raw_movies SET processed = TRUE WHERE processed = FALSE');

    return {
      total: insertedCount,
      duplicates: duplicates[0].count,
      unique: insertedCount - duplicates[0].count
    };

  } finally {
    await connection.end();
  }
}

module.exports = { deduplicateMovies };
