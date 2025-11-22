require("dotenv").config();
const mysql = require("mysql2/promise");
const logger = require("../../../utils/logger");

async function loadToWarehouse() {
  const stagingConn = await mysql.createConnection({
    host: process.env.DB_HOST || "localhost",
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USER || "root",
    password: process.env.DB_PASSWORD,
    database: process.env.STAGING_DB_NAME || "movie_staging",
  });

  const warehouseConn = await mysql.createConnection({
    host: process.env.DB_HOST || "localhost",
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USER || "root",
    password: process.env.DB_PASSWORD,
    database: process.env.WAREHOUSE_DB_NAME || "movie_dwh",
  });

  let logId = null;
  const startTime = new Date();
  let status = "success";
  let errorMessage = null;
  let loadedCount = 0;
  try {
    // Lấy movies hợp lệ từ staging (không duplicate, không có validation errors)
    const [validMovies] = await stagingConn.query(`
      SELECT s.* 
      FROM staging_movies s
      LEFT JOIN validation_errors v ON s.id = v.staging_id
      WHERE s.is_duplicate = 0
      AND v.id IS NULL
    `);
    logger.info(`Loading ${validMovies.length} valid movies to warehouse...`);
    for (const movie of validMovies) {
      // 1. Load genres
      if (movie.genre) {
        const genres = movie.genre.split(",").map((g) => g.trim());
        for (const genreName of genres) {
          await warehouseConn.query(
            `INSERT IGNORE INTO dim_genre (genre_name) VALUES (?)`,
            [genreName]
          );
        }
      }

      // 2. Load countries
      if (movie.origin_country) {
        const countries = movie.origin_country.split(",").map((c) => c.trim());
        for (const countryName of countries) {
          await warehouseConn.query(
            `INSERT IGNORE INTO dim_country (country_name) VALUES (?)`,
            [countryName]
          );
        }
      }

      // 3. Load persons (directors & actors)
      const persons = new Set();

      if (movie.director) {
        movie.director.split(",").forEach((d) => persons.add(d.trim()));
      }

      if (movie.actors) {
        movie.actors.split(",").forEach((a) => persons.add(a.trim()));
      }

      for (const personName of persons) {
        await warehouseConn.query(
          `INSERT IGNORE INTO dim_person (person_name) VALUES (?)`,
          [personName]
        );
      }

      // 4. Load fact_movie
      // Đảm bảo chỉ có 1 bản ghi is_current=TRUE cho mỗi tmdb_id + source
      const [existingMovie] = await warehouseConn.query(
        `SELECT * FROM fact_movie WHERE tmdb_id = ? AND source = ? AND is_current = TRUE`,
        [movie.tmdb_id, "kkphim"]
      );

      // Luôn update tất cả bản ghi cũ về is_current = FALSE
      await warehouseConn.query(
        `UPDATE fact_movie 
         SET valid_to = NOW(), is_current = FALSE 
         WHERE tmdb_id = ? AND source = ? AND is_current = TRUE`,
        [movie.tmdb_id, "kkphim"]
      );

      // Insert new record
      const [movieResult] = await warehouseConn.query(
        `INSERT INTO fact_movie (
          tmdb_id, source, title, description, poster_url, detail_url,
          total_episodes, quality, language, status, category,
          release_year, crawled_at, updated_at, episodes, is_current
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)`,
        [
          movie.tmdb_id,
          "kkphim",
          movie.title,
          movie.description,
          movie.poster,
          movie.detail_url,
          movie.total_episodes,
          movie.quality,
          movie.language,
          movie.status,
          movie.category,
          movie.release_year,
          movie.crawled_at,
          movie.updated_at,
          movie.episodes,
        ]
      );

      const movieKey = movieResult.insertId;

      // 5. Load bridge tables
      // Genre bridge
      if (movie.genre) {
        const genres = movie.genre.split(",").map((g) => g.trim());
        for (const genreName of genres) {
          const [genreRow] = await warehouseConn.query(
            "SELECT genre_key FROM dim_genre WHERE genre_name = ?",
            [genreName]
          );
          if (genreRow.length > 0) {
            await warehouseConn.query(
              `INSERT IGNORE INTO bridge_movie_genre (movie_key, genre_key) VALUES (?, ?)`,
              [movieKey, genreRow[0].genre_key]
            );
          }
        }
      }

      // Country bridge
      if (movie.origin_country) {
        const countries = movie.origin_country.split(",").map((c) => c.trim());
        for (const countryName of countries) {
          const [countryRow] = await warehouseConn.query(
            "SELECT country_key FROM dim_country WHERE country_name = ?",
            [countryName]
          );
          if (countryRow.length > 0) {
            await warehouseConn.query(
              `INSERT IGNORE INTO bridge_movie_country (movie_key, country_key) VALUES (?, ?)`,
              [movieKey, countryRow[0].country_key]
            );
          }
        }
      }

      // Person bridge (directors)
      if (movie.director) {
        const directors = movie.director.split(",").map((d) => d.trim());
        for (const directorName of directors) {
          const [personRow] = await warehouseConn.query(
            "SELECT person_key FROM dim_person WHERE person_name = ?",
            [directorName]
          );
          if (personRow.length > 0) {
            await warehouseConn.query(
              `INSERT IGNORE INTO bridge_movie_person (movie_key, person_key, role_type) 
               VALUES (?, ?, 'director')`,
              [movieKey, personRow[0].person_key]
            );
          }
        }
      }

      // Person bridge (actors)
      if (movie.actors) {
        const actors = movie.actors.split(",").map((a) => a.trim());
        for (const actorName of actors) {
          const [personRow] = await warehouseConn.query(
            "SELECT person_key FROM dim_person WHERE person_name = ?",
            [actorName]
          );
          if (personRow.length > 0) {
            await warehouseConn.query(
              `INSERT IGNORE INTO bridge_movie_person (movie_key, person_key, role_type) 
               VALUES (?, ?, 'actor')`,
              [movieKey, personRow[0].person_key]
            );
          }
        }
      }

      loadedCount++;
    }
    // Update processing step
    await stagingConn.query(`
      UPDATE staging_movies 
      SET processing_step = 'loaded'
      WHERE is_duplicate = FALSE
    `);
    logger.info(`Loaded ${loadedCount} movies to warehouse`);
    // Ghi log vào bảng processing_log
    const [logResult] = await stagingConn.query(
      `INSERT INTO processing_log (batch_id, step_name, status, records_processed, records_success, records_failed, start_time, end_time, error_message)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        `batch_${startTime.getTime()}`,
        "load_to_dw",
        status,
        validMovies.length,
        loadedCount,
        validMovies.length - loadedCount,
        startTime,
        new Date(),
        null,
      ]
    );
    logId = logResult.insertId;
    return { loaded: loadedCount, logId };
  } catch (error) {
    status = "failed";
    errorMessage = error.message || String(error);
    logger.error("Load to DW step failed:", error);
    // Ghi log lỗi vào bảng processing_log
    await stagingConn.query(
      `INSERT INTO processing_log (batch_id, step_name, status, records_processed, records_success, records_failed, start_time, end_time, error_message)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        `batch_${startTime.getTime()}`,
        "load_to_dw",
        status,
        0,
        0,
        0,
        startTime,
        new Date(),
        errorMessage,
      ]
    );
    throw error;
  } finally {
    await stagingConn.end();
    await warehouseConn.end();
  }
}

module.exports = { loadToWarehouse };
