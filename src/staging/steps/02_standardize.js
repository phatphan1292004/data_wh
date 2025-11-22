require('dotenv').config();
const mysql = require('mysql2/promise');
const logger = require('../../../utils/logger');

async function standardizeData() {
  const connection = await mysql.createConnection({
    host: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD,
    database: process.env.STAGING_DB_NAME || 'movie_staging'
  });

  let logId = null;
  const startTime = new Date();
  let status = 'success';
  let errorMessage = null;
  let standardizedCount = 0;
  try {
    // 1. Chuẩn hóa title: trim, loại bỏ khoảng trắng thừa
    await connection.query(`
      UPDATE staging_movies 
      SET title = TRIM(REGEXP_REPLACE(title, '\s+', ' '))
      WHERE is_duplicate = FALSE
    `);

    // 2. Chuẩn hóa release_year
    await connection.query(`
      UPDATE staging_movies 
      SET release_year = NULL
      WHERE release_year < 1900 OR release_year > YEAR(CURDATE()) + 2
    `);

    // 3. Chuẩn hóa genre, actors, director: trim
    await connection.query(`
      UPDATE staging_movies 
      SET genre = TRIM(genre),
          actors = TRIM(actors),
          director = TRIM(director),
          origin_country = TRIM(origin_country)
      WHERE is_duplicate = FALSE
    `);

    // 4. Uppercase quality
    await connection.query(`
      UPDATE staging_movies 
      SET quality = UPPER(quality)
      WHERE is_duplicate = FALSE
    `);

    // 5. Update processing step
    await connection.query(`
      UPDATE staging_movies 
      SET processing_step = 'standardize'
      WHERE is_duplicate = FALSE
    `);

    const [result] = await connection.query(
      'SELECT COUNT(*) as count FROM staging_movies WHERE is_duplicate = FALSE'
    );
    standardizedCount = result[0].count;
    logger.info(`Standardized ${standardizedCount} records`);

    // Ghi log vào bảng processing_log
    const [logResult] = await connection.query(
      `INSERT INTO processing_log (batch_id, step_name, status, records_processed, records_success, records_failed, start_time, end_time, error_message)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        `batch_${startTime.getTime()}`,
        'standardize',
        status,
        standardizedCount,
        standardizedCount,
        0,
        startTime,
        new Date(),
        null
      ]
    );
    logId = logResult.insertId;
    return { standardized: standardizedCount, logId };
  } catch (error) {
    status = 'failed';
    errorMessage = error.message || String(error);
    logger.error('Standardize step failed:', error);
    // Ghi log lỗi vào bảng processing_log
    await connection.query(
      `INSERT INTO processing_log (batch_id, step_name, status, records_processed, records_success, records_failed, start_time, end_time, error_message)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        `batch_${startTime.getTime()}`,
        'standardize',
        status,
        standardizedCount,
        0,
        standardizedCount,
        startTime,
        new Date(),
        errorMessage
      ]
    );
    throw error;
  } finally {
    await connection.end();
  }
}

module.exports = { standardizeData };
