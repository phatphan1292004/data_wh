
const { SystemMonitor, monitorSystem } = require('../../src/scheduler/monitor');
const { reportMovieStats } = require('./accesstool');

async function runAccessToolScheduler() {
  // 1. Kiểm tra hệ thống
  const healthReport = await monitorSystem();
  console.log('=== AccessTool Scheduler Health Report ===');
  console.log(JSON.stringify(healthReport, null, 2));

  // 2. Báo cáo dữ liệu warehouse
  console.log('\n=== Movie Warehouse Data Report ===');
  const stats = await reportMovieStats();
  console.log('Tổng số phim:', stats.totalMovies);
  console.log('Top thể loại:');
  stats.topGenres.forEach(g => console.log(`- ${g.genre_name}: ${g.count}`));
  console.log('Top quốc gia:');
  stats.topCountries.forEach(c => console.log(`- ${c.country_name}: ${c.count}`));

  // 3. Truy vấn sâu về chất lượng dữ liệu
  const mysql = require('mysql2/promise');
  const connection = await mysql.createConnection({
    host: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD,
    database: process.env.WAREHOUSE_DB_NAME || 'movie_dwh',
  });

  // Phim không có thể loại
  const [noGenre] = await connection.query(`
    SELECT COUNT(*) as count FROM fact_movie fm
    LEFT JOIN bridge_movie_genre bmg ON fm.movie_key = bmg.movie_key
    WHERE bmg.genre_key IS NULL
  `);
  // Phim không có quốc gia
  const [noCountry] = await connection.query(`
    SELECT COUNT(*) as count FROM fact_movie fm
    LEFT JOIN bridge_movie_country bmc ON fm.movie_key = bmc.movie_key
    WHERE bmc.country_key IS NULL
  `);
  // Phim không có diễn viên
  const [noActor] = await connection.query(`
    SELECT COUNT(*) as count FROM fact_movie fm
    LEFT JOIN bridge_movie_person bmp ON fm.movie_key = bmp.movie_key AND bmp.role_type = 'actor'
    WHERE bmp.person_key IS NULL
  `);
  // Phim thiếu thông tin quan trọng
  const [missingInfo] = await connection.query(`
    SELECT COUNT(*) as count FROM fact_movie
    WHERE title IS NULL OR poster_url IS NULL OR detail_url IS NULL
  `);

  await connection.end();

  console.log('\n=== Data Quality Deep Check ===');
  console.log(`Phim không có thể loại: ${noGenre[0].count}`);
  console.log(`Phim không có quốc gia: ${noCountry[0].count}`);
  console.log(`Phim không có diễn viên: ${noActor[0].count}`);
  console.log(`Phim thiếu thông tin quan trọng (title, poster, detail): ${missingInfo[0].count}`);
}

if (require.main === module) {
  runAccessToolScheduler()
    .then(() => process.exit(0))
    .catch(err => {
      console.error('❌ AccessTool Scheduler failed:', err);
      process.exit(1);
    });
}

module.exports = { runAccessToolScheduler };
