// AccessTool: Truy vấn, kiểm tra, báo cáo dữ liệu từ warehouse
require("dotenv").config();
const mysql = require("mysql2/promise");

async function queryWarehouse(sql, params = []) {
  const connection = await mysql.createConnection({
    host: process.env.DB_HOST || "localhost",
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USER || "root",
    password: process.env.DB_PASSWORD,
    database: process.env.WAREHOUSE_DB_NAME || "movie_dwh",
  });
  const [rows] = await connection.query(sql, params);
  await connection.end();
  return rows;
}

async function reportMovieStats() {
  const totalMovies = await queryWarehouse(
    "SELECT COUNT(*) as count FROM fact_movie"
  );
  const genres = await queryWarehouse(
    "SELECT genre_name, COUNT(*) as count FROM dim_genre JOIN bridge_movie_genre ON dim_genre.genre_key = bridge_movie_genre.genre_key GROUP BY genre_name ORDER BY count DESC LIMIT 10"
  );
  const countries = await queryWarehouse(
    "SELECT country_name, COUNT(*) as count FROM dim_country JOIN bridge_movie_country ON dim_country.country_key = bridge_movie_country.country_key GROUP BY country_name ORDER BY count DESC LIMIT 10"
  );

  return {
    totalMovies: totalMovies[0].count,
    topGenres: genres,
    topCountries: countries,
  };
}

async function runAccessTool() {
  console.log("📊 AccessTool: Movie Warehouse Report");
  const stats = await reportMovieStats();
  console.log("Tổng số phim:", stats.totalMovies);
  console.log("Top thể loại:");
  stats.topGenres.forEach((g) => console.log(`- ${g.genre_name}: ${g.count}`));
  console.log("Top quốc gia:");
  stats.topCountries.forEach((c) =>
    console.log(`- ${c.country_name}: ${c.count}`)
  );
}

if (require.main === module) {
  runAccessTool()
    .then(() => process.exit(0))
    .catch((err) => {
      console.error("❌ AccessTool failed:", err);
      process.exit(1);
    });
}

module.exports = { runAccessTool, reportMovieStats };
