require("dotenv").config();
const mysql = require("mysql2/promise");

/**
 * Get connection to movie_control database
 */
async function getControlConnection() {
  return await mysql.createConnection({
    host: process.env.DB_HOST || "localhost",
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USER || "root",
    password: process.env.DB_PASSWORD,
    database: process.env.CONTROL_DB_NAME || "movie_control",
  });
}

/**
 * Start a new processing log entry
 * @param {string} batchId - Unique batch identifier
 * @param {string} stepName - Name of the processing step
 */
async function startProcessLog(batchId, stepName) {
  const conn = await getControlConnection();
  try {
    await conn.query(
      `INSERT INTO processing_log (batch_id, step_name, status, start_time) 
       VALUES (?, ?, 'running', CURRENT_TIMESTAMP)`,
      [batchId, stepName]
    );
    console.log(`📝 Log started: ${batchId} - ${stepName}`);
  } catch (error) {
    console.error(`❌ Failed to start log: ${error.message}`);
  } finally {
    await conn.end();
  }
}

/**
 * @param {string} batchId - Unique batch identifier
 * @param {string} status - 'success' or 'failed'
 * @param {number} recordsProcessed - Total records processed
 * @param {number} recordsSuccess - Successfully processed records
 * @param {number} recordsFailed - Failed records
 * @param {string|null} errorMessage - Error message if failed
 */
async function endProcessLog(
  batchId,
  status = "success",
  recordsProcessed = 0,
  recordsSuccess = 0,
  recordsFailed = 0,
  errorMessage = null
) {
  const conn = await getControlConnection();
  try {
    await conn.query(
      `UPDATE processing_log 
       SET status = ?, 
           records_processed = ?, 
           records_success = ?, 
           records_failed = ?, 
           end_time = CURRENT_TIMESTAMP, 
           error_message = ?
       WHERE batch_id = ?`,
      [
        status,
        recordsProcessed,
        recordsSuccess,
        recordsFailed,
        errorMessage,
        batchId,
      ]
    );
    console.log(`✅ Log ended: ${batchId} - ${status}`);
  } catch (error) {
    console.error(`❌ Failed to end log: ${error.message}`);
  } finally {
    await conn.end();
  }
}

/**
 * @param {string} prefix
 * @returns {string}
 */
function generateBatchId(prefix = "batch") {
  const now = new Date();
  const timestamp = now
    .toISOString()
    .replace(/[-:T.]/g, "")
    .slice(0, 14);
  return `${prefix}_${timestamp}`;
}

module.exports = {
  startProcessLog,
  endProcessLog,
  generateBatchId,
  getControlConnection,
};
