require("dotenv").config();
const mysql = require("mysql2/promise");
const logger = require("../../../utils/logger");
const {
  startProcessLog,
  endProcessLog,
  generateBatchId,
} = require("../../control/utils/logger");

async function validateData() {
  const connection = await mysql.createConnection({
    host: process.env.DB_HOST || "localhost",
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USER || "root",
    password: process.env.DB_PASSWORD,
    database: process.env.STAGING_DB_NAME || "movie_staging",
  });

  const batchId = generateBatchId("validate");
  let validCount = 0;
  let invalidCount = 0;

  try {
    await startProcessLog(batchId, "validate_data");
    const [movies] = await connection.query(
      "SELECT * FROM staging_movies WHERE is_duplicate = FALSE"
    );
    for (const movie of movies) {
      const errors = [];
      // Validation rules
      if (!movie.title || movie.title.length < 1) {
        errors.push({
          type: "REQUIRED",
          field: "title",
          message: "Title is required",
        });
      }
      if (movie.title && movie.title.length > 500) {
        errors.push({
          type: "LENGTH",
          field: "title",
          message: "Title too long",
        });
      }
      if (
        movie.release_year &&
        (movie.release_year < 1900 || movie.release_year > 2030)
      ) {
        errors.push({
          type: "RANGE",
          field: "release_year",
          message: "Invalid release year",
        });
      }
      if (!movie.category) {
        errors.push({
          type: "REQUIRED",
          field: "category",
          message: "Category is required",
        });
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
    logger.info(
      `Validated: ${validCount} valid, ${invalidCount} invalid records`
    );

    await endProcessLog(
      batchId,
      "success",
      validCount + invalidCount,
      validCount,
      invalidCount
    );
    return {
      validCount,
      invalidCount,
      total: validCount + invalidCount,
      batchId,
    };
  } catch (error) {
    logger.error("Validate step failed:", error);
    await endProcessLog(
      batchId,
      "failed",
      validCount + invalidCount,
      validCount,
      invalidCount,
      error.message
    );
    throw error;
  } finally {
    await connection.end();
  }
}

module.exports = { validateData };
