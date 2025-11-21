require('dotenv').config();
const mysql = require('mysql2/promise');
const fs = require('fs').promises;
const path = require('path');

async function loadRawToStaging() {
  const rawDir = path.join(__dirname, '../../data/raw');
  const connection = await mysql.createConnection({
    host: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD,
    database: process.env.DB_STAGING || 'movie_staging',
  });

  try {
    const files = await fs.readdir(rawDir);
    for (const file of files) {
      if (file.endsWith('.json')) {
        const filePath = path.join(rawDir, file);
        const data = JSON.parse(await fs.readFile(filePath, 'utf8'));
        // Lấy đúng mảng phim từ thuộc tính 'data' trong file JSON
        const movies = Array.isArray(data) ? data : data.data;
        for (const movie of movies) {
          await connection.query(
            'INSERT INTO raw_movies SET ?',
            [{
              source: 'kkphim',
              raw_data: JSON.stringify(movie),
              processed: false
            }]
          );
        }
        console.log(`✅ Loaded ${file} into staging database.`);
      }
    }
    console.log('🎉 All raw data loaded to staging!');
  } catch (error) {
    console.error('❌ Error loading raw data:', error);
    throw error;
  } finally {
    await connection.end();
  }
}

loadRawToStaging().catch(err => {
  console.error(err);
  process.exit(1);
});
