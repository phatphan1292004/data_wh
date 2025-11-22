require('dotenv').config();
const mysql = require('mysql2/promise');
const fs = require('fs').promises;
const path = require('path');

async function setupDatabases() {
  console.log('🚀 Setting up databases...');
  
  const connection = await mysql.createConnection({
    host: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD,
    multipleStatements: true
  });

  let logId = null;
  const startTime = new Date();
  let status = 'success';
  let errorMessage = null;
  try {
    // 1. Setup Staging Database
    console.log('📦 Creating staging database...');
    const stagingSQL = await fs.readFile(
      path.join(__dirname, '../../src/staging/models/create-tables.sql'),
      'utf8'
    );
    await connection.query(stagingSQL);
    console.log('✅ Staging database created');

    // 2. Setup Warehouse Database
    console.log('🏢 Creating warehouse schema...');
    const schemaFiles = [
      'dim_date.sql',
      'dim_genre.sql',
      'dim_country.sql',
      'dim_person.sql',
      'fact_movie.sql'
    ];
    for (const file of schemaFiles) {
      const sql = await fs.readFile(
        path.join(__dirname, `../../src/warehouse/schema/${file}`),
        'utf8'
      );
      await connection.query(sql);
      console.log(`✅ Created ${file}`);
    }
    console.log('🎉 Database setup completed successfully!');
    // Ghi log vào bảng processing_log trong movie_staging
    const logConn = await mysql.createConnection({
      host: process.env.DB_HOST || 'localhost',
      port: process.env.DB_PORT || 3306,
      user: process.env.DB_USER || 'root',
      password: process.env.DB_PASSWORD,
      database: process.env.STAGING_DB_NAME || 'movie_staging'
    });
    await logConn.query(
      `INSERT INTO processing_log (batch_id, step_name, status, records_processed, records_success, records_failed, start_time, end_time, error_message)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        `batch_${startTime.getTime()}`,
        'setup_db',
        status,
        0,
        0,
        0,
        startTime,
        new Date(),
        null
      ]
    );
    await logConn.end();
  } catch (error) {
    status = 'failed';
    errorMessage = error.message || String(error);
    console.error('❌ Error setting up databases:', error);
    // Ghi log lỗi vào bảng processing_log trong movie_staging
    const logConn = await mysql.createConnection({
      host: process.env.DB_HOST || 'localhost',
      port: process.env.DB_PORT || 3306,
      user: process.env.DB_USER || 'root',
      password: process.env.DB_PASSWORD,
      database: process.env.STAGING_DB_NAME || 'movie_staging'
    });
    await logConn.query(
      `INSERT INTO processing_log (batch_id, step_name, status, records_processed, records_success, records_failed, start_time, end_time, error_message)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        `batch_${startTime.getTime()}`,
        'setup_db',
        status,
        0,
        0,
        0,
        startTime,
        new Date(),
        errorMessage
      ]
    );
    await logConn.end();
    throw error;
  } finally {
    await connection.end();
  }
}

setupDatabases().catch(err => {
  console.error(err);
  process.exit(1);
});
