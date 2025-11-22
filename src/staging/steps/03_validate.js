require('dotenv').config();
const mysql = require('mysql2/promise');
const logger = require('../../../utils/logger');

async function validateData() {
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
  let validCount = 0;
  let invalidCount = 0;
  try {
    const [movies] = await connection.query(
      'SELECT * FROM staging_movies WHERE is_duplicate = FALSE'
    );
    for (const movie of movies) {
      const errors = [];
      // Validation rules
      if (!movie.title || movie.title.length < 1) {
        errors.push({ type: 'REQUIRED', field: 'title', message: 'Title is required' });
      }
      if (movie.title && movie.title.length > 500) {
        errors.push({ type: 'LENGTH', field: 'title', message: 'Title too long' });
      }
      if (movie.release_year && (movie.release_year < 1900 || movie.release_year > 2030)) {
        errors.push({ type: 'RANGE', field: 'release_year', message: 'Invalid release year' });
      }
      if (!movie.category) {
        errors.push({ type: 'REQUIRED', field: 'category', message: 'Category is required' });
      }
      // Insert errors vào validation_errors table
      if (errors.length > 0) {
        invalidCount++;
        for (const error of errors) {
          await connection.query(
            `INSERT INTO validation_errors (staging_id, error_type, field_name, error_message)
             VALUES (?, ?, ?, ?)`,
            [movie.id, error.type, error.field, error.message]
          );
        }
      } else {
        validCount++;
      }
    }
    // Update processing step
    await connection.query(`
      UPDATE staging_movies 
      SET processing_step = 'validate'
      WHERE is_duplicate = FALSE
    `);
    logger.info(`Validated: ${validCount} valid, ${invalidCount} invalid records`);
    // Ghi log vào bảng processing_log
    const [logResult] = await connection.query(
      `INSERT INTO processing_log (batch_id, step_name, status, records_processed, records_success, records_failed, start_time, end_time, error_message)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        `batch_${startTime.getTime()}`,
        'validate',
        status,
        validCount + invalidCount,
        validCount,
        invalidCount,
        startTime,
        new Date(),
        null
      ]
    );
    logId = logResult.insertId;
    return { 
      validCount, 
      invalidCount, 
      total: validCount + invalidCount,
      logId
    };
  } catch (error) {
    status = 'failed';
    errorMessage = error.message || String(error);
    logger.error('Validate step failed:', error);
    // Ghi log lỗi vào bảng processing_log
    await connection.query(
      `INSERT INTO processing_log (batch_id, step_name, status, records_processed, records_success, records_failed, start_time, end_time, error_message)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        `batch_${startTime.getTime()}`,
        'validate',
        status,
        validCount + invalidCount,
        validCount,
        invalidCount,
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

module.exports = { validateData };
